"""Project local events into deduplicated, content free phone notifications."""

from __future__ import annotations

import json
from datetime import datetime

from .phone_control import button, packet
from .phone_store import PhoneError


class PhoneNotifications:
    def __init__(self, control):
        self.control, self.store = control, control.store

    def project(self, now):
        if not self.control.policy.notifications:
            return
        labels = {
            "waiting_input": "Input needed",
            "review_ready": "Review needed",
            "waiting_approval": "Approval needed",
            "blocked": "Blocked",
            "failed": "Failed",
            "completed": "Completed",
        }
        with self.store._write_connection() as connection:
            cursor = self.store.cursor(connection)
            records = connection.execute(
                """SELECT e.id,e.job_id,e.event_type,e.payload,j.project_id,j.state,j.updated_at
                FROM events e JOIN jobs j ON j.id=e.job_id WHERE e.id>? ORDER BY e.id LIMIT 200""",
                (cursor["event_id"],),
            ).fetchall()
            for row in records:
                if row["project_id"] not in self.control.policy.projects:
                    continue
                try:
                    self.control.project(row["project_id"], connection)
                except PhoneError:
                    continue
                if row["event_type"] == "native.reconciliation.required":
                    attempt_id = json.loads(row["payload"]).get("attempt_id")
                    if self.reconciliation(connection, row["job_id"], attempt_id):
                        self.store.queue(
                            connection,
                            f"reconcile:{attempt_id}",
                            [
                                packet(
                                    f"{self.control.alias(row['project_id'])} / "
                                    f"Task {self.control.task_alias(row['job_id'])}"
                                    "\nRun needs reconciliation on the Mac.",
                                    [
                                        [
                                            button(
                                                "Open task",
                                                f"task:{self.control.task_alias(row['job_id'])}",
                                            )
                                        ]
                                    ],
                                )
                            ],
                            now + 86400,
                            project=row["project_id"],
                            kind="notice",
                            subject={"job_id": row["job_id"], "attempt_id": attempt_id},
                        )
                    continue
                if row["state"] not in labels:
                    continue
                expires = int(datetime.fromisoformat(row["updated_at"]).timestamp()) + 86400
                if row["state"] == "waiting_approval":
                    approval = connection.execute(
                        "SELECT expires_at FROM approvals WHERE job_id=? AND decision IS NULL "
                        "ORDER BY created_at DESC LIMIT 1",
                        (row["job_id"],),
                    ).fetchone()
                    if approval is None:
                        continue
                    expires = min(expires, approval["expires_at"] // 1_000_000)
                if expires <= now:
                    continue
                short = self.control.task_alias(row["job_id"])
                self.store.queue(
                    connection,
                    f"notice:{row['job_id']}:{row['state']}:{row['updated_at']}",
                    [
                        packet(
                            f"{self.control.alias(row['project_id'])} / Task {short}"
                            f"\n{labels[row['state']]}",
                            [[button("Open task", f"task:{short}")]],
                        )
                    ],
                    expires,
                    project=row["project_id"],
                    kind="notice",
                    subject={
                        "job_id": row["job_id"],
                        "state": row["state"],
                        "updated_at": row["updated_at"],
                    },
                )
            if records:
                connection.execute(
                    "UPDATE phone_cursors SET event_id=? WHERE scope_hash=?",
                    (records[-1]["id"], self.store.scope),
                )
            marks = ",".join("?" for _ in self.control.policy.projects)
            for row in connection.execute(
                "SELECT id,project_id,due_at FROM planner WHERE kind='deadline' "
                f"AND promoted_job_id IS NULL AND project_id IN ({marks}) "
                "AND due_at BETWEEN ? AND ? ORDER BY due_at LIMIT 100",
                (*self.control.policy.projects, (now - 300) * 1_000_000, (now + 3600) * 1_000_000),
            ).fetchall():
                self.store.queue(
                    connection,
                    f"deadline:{row['id']}:{row['due_at']}",
                    [
                        packet(
                            f"{self.control.alias(row['project_id'])}"
                            "\nA deadline is due within one hour or has just passed."
                            " Open Planner on the Mac."
                        )
                    ],
                    row["due_at"] // 1_000_000 + 300,
                    project=row["project_id"],
                    kind="notice",
                    subject={"planner_id": row["id"], "due_at": row["due_at"]},
                )

    def eligible(self, connection, row):
        if row["project_id"] is not None:
            try:
                self.control.project(row["project_id"], connection)
            except PhoneError:
                return False
        subject = json.loads(row["subject"])
        if row["kind"] == "review":
            if not self.control.policy.review_content:
                return False
            current = self.control.state.approval_service.read(subject["job_id"])
            return (
                current["review_current"]
                and current["bundle_hash"] == subject["bundle_hash"]
                and current["approval"] is not None
                and current["approval"]["approval_id"] == subject["approval_id"]
                and current["approval"]["state"] == "pending"
            )
        if row["kind"] != "notice":
            return True
        if not self.control.policy.notifications:
            return False
        if "attempt_id" in subject:
            return self.reconciliation(connection, subject["job_id"], subject["attempt_id"])
        if "job_id" in subject:
            current = connection.execute(
                "SELECT state,updated_at FROM jobs WHERE id=?", (subject["job_id"],)
            ).fetchone()
            return (
                current is not None
                and current["state"] == subject["state"]
                and current["updated_at"] == subject["updated_at"]
            )
        current = connection.execute(
            "SELECT due_at,promoted_job_id FROM planner WHERE id=?", (subject["planner_id"],)
        ).fetchone()
        return (
            current is not None
            and current["due_at"] == subject["due_at"]
            and current["promoted_job_id"] is None
        )

    @staticmethod
    def reconciliation(connection, job_id, attempt_id):
        return (
            connection.execute(
                "SELECT 1 FROM native_attempts "
                "WHERE job_id=? AND id=? AND held=1 AND phase='reconcile'",
                (job_id, attempt_id),
            ).fetchone()
            is not None
        )

    def next(self, bot_id, owner_id):
        self.control.require(bot_id, owner_id)
        now = int(self.control.clock())
        self.project(now)
        return self.store.delivery(now, self.eligible)
