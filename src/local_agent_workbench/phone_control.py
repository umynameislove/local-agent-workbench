"""Owner scoped phone commands over the existing local task and review services."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from datetime import datetime

from .db import ProjectRepository
from .engine import JobCreate, JobRuntime, JobState, WorkbenchConfig
from .phone_store import PhoneError, PhoneStore
from .remote_commands import RemoteJournal
from .secret_gate import SecretScanner


def button(text, data):
    return {"text": text, "callback_data": data}


def packet(text, rows=()):
    return {"text": text, **({"reply_markup": {"inline_keyboard": list(rows)}} if rows else {})}


class PhoneControl:
    def __init__(self, state, policy, *, clock=time.time):
        self.state, self.policy, self.clock = state, policy, clock
        self.store = PhoneStore(state.database, policy)
        self.journal = RemoteJournal(state.database)

    def require(self, bot_id, owner_id=...):
        if not self.policy.enabled or type(bot_id) is not int or bot_id != self.policy.bot_id:
            raise PhoneError("Phone control is disabled or bot identity differs.")
        if owner_id is not ... and (type(owner_id) is not int or owner_id != self.policy.owner_id):
            raise PhoneError("Phone owner differs.")
        if WorkbenchConfig.load(self.state.runtime.config).phone_control != self.policy:
            raise PhoneError("Phone policy changed. Restart the gateway and backend.")

    def project(self, identity, connection=None):
        if identity not in self.policy.projects:
            raise PhoneError("Project is outside phone control scope.")
        if connection is None:
            project = self.state.project_repository.get(identity)
        else:
            row = ProjectRepository._select_by_id(connection, identity)
            if row is None:
                raise PhoneError("Project is unavailable.")
            project = ProjectRepository._to_record(row)
        if project.sensitivity.value not in {"public", "private"}:
            raise PhoneError("Project is local only.")
        return project

    def alias(self, project):
        return f"Project {self.policy.projects.index(project) + 1}"

    @staticmethod
    def task_alias(job_id):
        return hashlib.sha256(job_id.encode()).hexdigest()[:10]

    def task(self, short):
        matches = []
        for project in self.policy.projects:
            self.project(project)
            with self.store._connection() as connection:
                matches.extend(
                    row["id"]
                    for row in connection.execute(
                        "SELECT id FROM jobs WHERE project_id=?", (project,)
                    )
                    if self.task_alias(row["id"]) == short
                )
        if len(matches) != 1:
            raise PhoneError("Task is unavailable. Open Tasks again.")
        return self.state.job_repository.get(matches[0])

    def configuration(self, bot_id):
        self.require(bot_id)
        return {
            "owner_id": self.policy.owner_id,
            "bot_id": self.policy.bot_id,
            "projects": list(self.policy.projects),
            "scope": self.policy.scope,
            "review_content": self.policy.review_content,
            "notifications": self.policy.notifications,
        }

    async def process(self, bot_id, update):
        self.require(bot_id)
        if not isinstance(update, dict):
            raise PhoneError("Update is invalid.")
        now = int(self.clock())
        receipt, _ = self.journal.receive(str(bot_id), update, self.policy.scope, now)
        if receipt["phase"] != "received" or receipt["scope_hash"] != self.policy.scope:
            return {"accepted": True}
        message = self.policy.owner_message(update, now)
        if message is None:
            with self.store._write_connection() as connection:
                self.store.complete_receipt(connection, str(bot_id), update["update_id"])
            return {"accepted": True}
        try:
            callback = update.get("callback_query")
            if callback and str(callback.get("data", "")).startswith("act:"):
                return await self.perform(update, message)
            result, action, project, kind = await self.describe(update, message, now)
            with self.store._write_connection() as connection:
                if not self.store.receipt_pending(connection, update["update_id"]):
                    return {"accepted": True}
                token, expires = None, now + 120
                if action:
                    name, data, expires = action[:3]
                    if len(action) == 4:
                        if self.store.action(action[3], connection)["phase"] == "done":
                            self.store.complete_receipt(
                                connection, str(bot_id), update["update_id"]
                            )
                            return {"accepted": True}
                        self.store.finish_action(connection, action[3], "Request prepared.")
                    token = self.store.add_action(connection, name, data, expires)
                    if name == "compose":
                        result = [
                            dict(
                                packet(
                                    "Reply to this message with your task. No worker starts yet."
                                ),
                                reply_markup={"force_reply": True, "selective": True},
                            )
                        ]
                    elif name == "decision":
                        result[-1]["reply_markup"] = {
                            "inline_keyboard": [
                                [
                                    button("Approve", f"act:approved:{token}"),
                                    button("Changes", f"act:changes_requested:{token}"),
                                    button("Reject", f"act:rejected:{token}"),
                                ]
                            ]
                        }
                    else:
                        result[-1]["reply_markup"] = {
                            "inline_keyboard": [
                                [
                                    button("Confirm", f"act:confirm:{token}"),
                                    button("Discard", f"act:discard:{token}"),
                                ]
                            ]
                        }
                subject = action[1] if action and action[0] == "decision" else None
                self.store.queue(
                    connection,
                    f"update:{update['update_id']}",
                    result,
                    expires,
                    project=project,
                    kind=kind,
                    token=token,
                    subject=subject,
                )
                self.store.complete_receipt(connection, str(bot_id), update["update_id"])
        except (ValueError, RuntimeError, IndexError, KeyError, TypeError):
            with self.store._write_connection() as connection:
                if self.store.receipt_pending(connection, update["update_id"]):
                    self.store.queue(
                        connection,
                        f"update:{update['update_id']}",
                        [packet("Request expired or unavailable. Use /start or open the Mac.")],
                        now + 120,
                    )
                    self.store.complete_receipt(connection, str(bot_id), update["update_id"])
        return {"accepted": True}

    async def describe(self, update, message, now):
        callback = update.get("callback_query")
        command = callback.get("data") if callback else message.get("text", "")
        if not isinstance(command, str) or len(command.encode()) > 16384:
            raise PhoneError("Command is invalid.")
        if not callback and message.get("reply_to_message"):
            if SecretScanner.text_findings(command):
                raise PhoneError("Sensitive request content must stay on the Mac.")
            if len(command.encode("utf-16-le")) // 2 > 2500:
                raise PhoneError("Task is too long. Use at most 2500 characters.")
            prompt = message["reply_to_message"]
            if (
                not isinstance(prompt, dict)
                or prompt.get("from", {}).get("id") != self.policy.bot_id
            ):
                raise PhoneError("Reply does not belong to this bot.")
            token = self.store.sent_prompt(prompt.get("message_id"))
            action = self.store.action(token)
            if action["kind"] != "compose" or action["expires_at"] <= now:
                raise PhoneError("Task prompt expired.")
            action = self.store.begin(
                token,
                "reply:" + hashlib.sha256(command.encode()).hexdigest(),
                prompt["message_id"],
                now,
            )
            if action["phase"] == "done":
                raise PhoneError("Task prompt was already consumed.")
            project, agent = action["data"]["project_id"], action["data"]["agent_id"]
            self.project(project)
            job = await self.state.job_submission_service.prepare(
                {"project_id": project, "agent_id": agent, "request": command}
            )
            if job.request_snapshot["agent_binding"] != action["data"]["binding"]:
                raise PhoneError("Agent binding changed. Select it again.")
            data = asdict(job)
            return (
                [
                    packet(
                        f"{self.alias(project)} / Task {self.task_alias(job.id)}\n{command}"
                        "\nCreate this task? Execution stays manual."
                    )
                ],
                ("submit", data, now + 120, token),
                project,
                "reply",
            )
        if command in ("/mute", "/unmute"):
            with self.store._write_connection() as connection:
                self.store.cursor(connection)
                connection.execute(
                    "UPDATE phone_cursors SET muted=? WHERE scope_hash=?",
                    (int(command == "/mute"), self.policy.scope),
                )
            return (
                [
                    packet(
                        "Notifications muted."
                        if command == "/mute"
                        else "Notifications enabled within configured quiet hours."
                    )
                ],
                None,
                None,
                "reply",
            )
        if command in ("/start", "/help", "/status", "/tasks", "/needs"):
            rows = []
            for n, project in enumerate(self.policy.projects, 1):
                self.project(project)
                rows.append([button(self.alias(project), f"project:{n}")])
            return (
                [
                    packet(
                        "Choose a project.\n/mute pauses notifications; /unmute restores them.",
                        rows,
                    )
                ],
                None,
                None,
                "reply",
            )
        if callback and command.startswith("project:"):
            number = int(command.split(":")[1])
            if number < 1:
                raise PhoneError("Project is invalid.")
            project = self.policy.projects[number - 1]
            self.project(project)
            agents = self.state.task_workflow.agents(project)["agents"]
            rows = [
                [
                    button(
                        f"New task / Agent {n}",
                        f"compose:{self.policy.projects.index(project) + 1}:{n}",
                    )
                ]
                for n, _ in enumerate(agents, 1)
            ]
            with self.store._connection() as connection:
                jobs = connection.execute(
                    "SELECT * FROM jobs WHERE project_id=? "
                    "ORDER BY created_at DESC,id DESC LIMIT 8",
                    (project,),
                ).fetchall()
            for row in jobs:
                rows.append(
                    [
                        button(
                            f"Task {self.task_alias(row['id'])} / {row['state'].replace('_', ' ')}",
                            f"task:{self.task_alias(row['id'])}",
                        )
                    ]
                )
            return (
                [
                    packet(
                        f"{self.alias(project)}\n{len(agents)} profiles. Latest 8 tasks.",
                        rows,
                    )
                ],
                None,
                project,
                "reply",
            )
        if callback and command.startswith("compose:"):
            _, p, a = command.split(":")
            if int(p) < 1 or int(a) < 1:
                raise PhoneError("Profile is invalid.")
            project = self.policy.projects[int(p) - 1]
            self.project(project)
            agent = self.state.task_workflow.agents(project)["agents"][int(a) - 1]
            binding = self.state.team_repository.snapshot(project, agent["id"])
            return (
                [],
                (
                    "compose",
                    {"project_id": project, "agent_id": agent["id"], "binding": binding},
                    now + 120,
                ),
                project,
                "reply",
            )
        if callback and command.startswith("task:"):
            job = self.task(command.split(":")[1])
            attempt = self.state.native_attempt_repository.get(job.id)
            if attempt and attempt.phase in {"admitted", "running", "reconcile"}:
                return (
                    [
                        packet(
                            f"Task {self.task_alias(job.id)} / {attempt.phase}"
                            "\nRequest cancellation of this exact run?"
                        )
                    ],
                    (
                        "cancel",
                        {"job_id": job.id, "project_id": job.project_id, "attempt_id": attempt.id},
                        now + 120,
                    ),
                    job.project_id,
                    "reply",
                )
            status = self.state.approval_service.read(job.id)
            if status["approval"] and status["approval"]["state"] == "pending":
                return self.review(job, status, now)
            return (
                [packet(f"Task {self.task_alias(job.id)} / {job.state.value.replace('_', ' ')}")],
                None,
                job.project_id,
                "reply",
            )
        raise PhoneError("Command is unsupported.")

    def review(self, job, status, now):
        if not self.policy.review_content:
            return (
                [
                    packet(
                        f"Task {self.task_alias(job.id)} needs review on the Mac."
                        " Telegram review content is disabled."
                    )
                ],
                None,
                job.project_id,
                "reply",
            )
        bundle = self.state.review_bundle_repository.get(job.id)
        data = bundle.payload
        if (
            not status["review_current"]
            or bundle.payload_hash != status["bundle_hash"]
            or data["diff"]["truncated"]
            or any(f["patch"] is None for f in data["diff"]["files"])
        ):
            raise PhoneError("Complete review is unavailable. Continue on the Mac.")
        approval = status["approval"]
        expires = min(now + 120, int(datetime.fromisoformat(approval["expires_at"]).timestamp()))
        if expires <= now:
            raise PhoneError("Approval expired.")
        text = (
            f"Frozen review / Task {self.task_alias(job.id)}\nSnapshot {bundle.payload_hash}"
            f"\nApproval {approval['approval_id']}\nExpires {approval['expires_at']}\n"
        )
        text += "\nWarnings\n" + json.dumps(data["risks"], ensure_ascii=False)
        text += "\nTask\n" + data["summary"]["task"]
        text += "\nBase commit\n" + data["diff"]["base_commit"]
        text += "\nChecks\n" + json.dumps(data["verification"], ensure_ascii=False)
        for file in data["diff"]["files"]:
            text += f"\n{file['status']} {file['path']}\n{file['patch']}\n"
        text += (
            "\nApprove records permission for this snapshot only. Apply locally remains separate."
        )
        if SecretScanner.text_findings(text):
            raise PhoneError("Sensitive review content must stay on the Mac.")
        if len(text.encode("utf-16-le")) // 2 > 20000:
            raise PhoneError("Review is too large. Continue on the Mac.")
        parts, chunk = [], ""
        for character in text:
            if len((chunk + character).encode("utf-16-le")) // 2 > 2800:
                parts.append(packet(chunk))
                chunk = ""
            chunk += character
        parts.append(packet(chunk))
        return (
            parts,
            (
                "decision",
                {
                    "project_id": job.project_id,
                    "job_id": job.id,
                    "approval_id": approval["approval_id"],
                    "bundle_hash": bundle.payload_hash,
                },
                expires,
            ),
            job.project_id,
            "review",
        )

    async def perform(self, update, message):
        _, choice, token = update["callback_query"]["data"].split(":")
        initial = self.store.action(token)
        allowed = {
            "submit": {"confirm", "discard"},
            "cancel": {"confirm", "discard"},
            "decision": {"approved", "changes_requested", "rejected"},
        }
        if choice not in allowed.get(initial["kind"], set()):
            raise PhoneError("Action choice is invalid.")
        action = self.store.begin(token, choice, message["message_id"], int(self.clock()))
        data = action["data"]
        self.project(data["project_id"])
        actor = f"telegram:{self.policy.owner_id}"
        if action["phase"] == "done":
            outcome = action["outcome"]
        elif choice == "discard":
            outcome = "Action discarded."
        elif action["kind"] == "submit" and choice == "confirm":
            job = JobCreate(
                **{**data, "runtime": JobRuntime(data["runtime"]), "state": JobState(data["state"])}
            )
            if int(self.clock()) >= action["expires_at"]:
                stored = self.state.job_repository.get(job.id)
                if stored.request_snapshot != job.request_snapshot or stored.request != job.request:
                    raise PhoneError("Task outcome is unresolved.")
            else:
                stored = self.state.job_submission_service.commit_prepared(job, actor)
            outcome = f"Task {self.task_alias(stored.id)} created. Worker has not been started."
        elif action["kind"] == "cancel" and choice == "confirm":
            attempt = self.state.native_attempt_repository.get(data["job_id"])
            if not attempt or attempt.id != data["attempt_id"]:
                raise PhoneError("Run changed.")
            if int(self.clock()) >= action["expires_at"] and not attempt.cancel_requested:
                raise PhoneError("Cancellation confirmation expired.")
            self.state.native_attempt_repository.cancel(
                data["job_id"], data["attempt_id"], actor=actor, channel="telegram"
            )
            outcome = (
                "Cancellation requested. Process termination is not yet confirmed."
                if attempt.phase != "settled"
                else f"Run already settled: {attempt.outcome}."
            )
        elif action["kind"] == "decision" and choice in {
            "approved",
            "changes_requested",
            "rejected",
        }:
            current = self.state.approval_repository.get(data["approval_id"])
            if int(self.clock()) >= action["expires_at"] and current.decision is None:
                raise PhoneError("Decision confirmation expired.")
            await self.state.approval_service.decide(
                data["approval_id"],
                {"decision": choice, "bundle_hash": data["bundle_hash"]},
                actor=actor,
                channel="telegram",
            )
            outcome = f"Decision recorded: {choice.replace('_', ' ')}. No patch was applied."
        else:
            raise PhoneError("Action choice is invalid.")
        with self.store._write_connection() as connection:
            self.store.finish_action(connection, token, outcome)
            if self.store.receipt_pending(connection, update["update_id"]):
                self.store.queue(
                    connection,
                    f"update:{update['update_id']}",
                    [packet(outcome)],
                    int(self.clock()) + 120,
                    project=data["project_id"],
                )
                self.store.complete_receipt(
                    connection, str(self.policy.bot_id), update["update_id"]
                )
        return {"accepted": True}
