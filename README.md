# Local Agent Workbench

> A local AI workbench for project specific agent teams, durable native execution and reviewed local changes.

Local Agent Workbench is an open source macOS application for running real project work through multiple AI runtimes from one persistent app. It is not another chat wrapper: every task has durable state, an isolated worktree, an explainable route, a review bundle and an explicit promotion gate.

## Product in one minute

The target product opens at login and exposes two native windows. The current implementation is a local browser workspace, not the native shell:

1. **AI Workspace:** project chat, `Auto / Claude / Codex / Local`, plan, tool stream, files, diff, tests, approvals and provider health.

2. **Planner:** calendar, deadlines, job queue, `Needs you`, watcher inbox, notifications and the audit ledger.

The default flow is:

```text
task → local policy check → plan → isolated worktree → execute
     → verify → review → human approval → apply locally → remember
```

## What makes it different

1. Native Claude Code and Codex runtimes use the user's subscriptions; this project does not turn them into paid API calls.

2. Version two configuration describes connections, models, agent profiles and project teams. OpenRouter inference and API tool execution remain unavailable. Legacy advice configuration never grants a worker role or spending authority through migration.

3. A local Qwen model suggests classifications and handles private or repetitive work; deterministic backend policy owns routing and fails closed when cloud use is forbidden.

4. Switching runtimes creates a checked handoff package instead of pretending providers share hidden context.

5. `projmem` MCP is the durable project memory engine; SQLite owns operational state only.

6. Agents can write freely inside isolated worktrees, but cannot promote, push, deploy, send or spend without the appropriate gate.

7. No account, no hosted relay and no telemetry by default.

## Source and local state

This repository contains only public, reproducible source. Personal hosting state lives outside the repo:

```text
working/        public source; commit and push this
working-local/  real config, database, logs, cache and worktrees; never push
```

The application will use `AGENT_WORKBENCH_HOME` to locate its runtime home. Personal configuration and operational state are never part of this repository.

## Repository layout

```text
src/local_agent_workbench/  application modules and packaged workspace assets
tests/                     Python and browser controller tests
scripts/                   repository maintenance tools
.github/workflows/         continuous integration
```

Install the project with `uv sync --frozen` before running commands. The editable development install and published wheel use the same `local_agent_workbench` package. `uv run agent-workbench` and `uv run python -m local_agent_workbench` start the local server. The former root module commands are replaced by these package entry points. Runtime configuration and SQLite data retain their existing locations.

## Project configuration

Copy the public example into the runtime home, then set each project root to an existing Git worktree:

```bash
cp config.example.json ../working-local/config.json
```

Relative project roots are resolved from the directory containing `config.json`. Each root must identify the top level directory of a Git worktree. Missing paths, regular files, non Git directories and nested worktree paths stop startup with a safe configuration error.

The application can start without `config.json` and reports an empty project list. When configuration is present, matching projects are registered once in SQLite. A later configuration that conflicts with persisted project identity or policy fails closed instead of rewriting durable state.

Submit a task with `POST /api/jobs`. The client provides `project_id`, `request` and optional `runtime`, `model` and `agent_id` values. An explicit agent must belong to the selected project team; runtime and model preferences cannot override that profile. The server verifies repository `HEAD` and freezes project policy plus the resolved team, profile, source and model. Legacy submissions remain readable but do not gain execution authority from later settings. Responses omit project roots, account references and internal snapshots.

### Project teams and native execution

Version one configuration remains supported without enabling native execution. `local_agent_workbench.team_config.migrate_config` returns a version two copy with an empty agent system, native execution disabled and legacy advice disabled. It does not write a file, discover accounts or modify credentials. The public example intentionally stays disabled and compatible with version one.

In version two, `agent_system` contains `connections`, `models`, `agents`, `teams` and explicit `native_execution_enabled`. The default global limit is four active agents with one heavy local lane. The first supervisor is deliberately stricter: one native writer across the database. Higher fleet capacity is not yet implemented. Local configuration must identify a native account slot and its verified capabilities; no browser response contains a credential reference or account directory. A source connection alone never permits project access. Native subscription workers are cloud runtimes and cannot execute for cloud prohibited projects.

`GET /api/projects/{id}/team` reads the current roster and revision. `PUT` takes exactly `revision` and `team`; revision zero creates a previously absent team, and a stale revision returns conflict. Initial configuration seeds a missing team only once, so local API edits survive restart. Disabling a team or removing its active agent records cancellation for its owned native attempt. Snapshots do not expand after an edit, and revocation still prevents further execution. Team settings UI and automatic Lead delegation are not included yet.

The native supervisor currently supports a Codex implementer with the `native_workspace` tool and explicit relative directory write scopes. It rejects API models, arbitrary native tool subsets, document scoping and step/token limits it cannot enforce. Model metadata is an operator configuration assertion, not a model quality or quota measurement.

After explicit agent submission, planning and worktree preparation, `GET /api/jobs/{id}/execution` returns the current authorization hash and any recorded attempt. `POST` to the same route takes exactly `authorization_hash`; a matching request starts only when local configuration explicitly enables native execution. No task starts automatically at submission or application startup. The adapter verifies required CLI options and accepts only recognized ChatGPT login mode. API key mode and unknown authentication output fail closed. This is an authentication method check, not proof of plan entitlement, remaining quota or a successful live provider run. The selected account directory must be exclusively managed by the operator; an external account change cannot be fenced by the local database.

Admission persists an attempt, fixed account slot, worktree identity and authority before spawning. Concurrent or duplicate starts observe that attempt rather than launching again. Native events enter the durable SSE ledger. A coherent successful native turn moves the task to verification, never directly to approval or promotion. Reported completion followed by a failed process exit is failure, not success.

`POST /api/jobs/{id}/execution/cancel` takes exactly `attempt_id`. Cancellation intent is recorded before process termination. The supervisor also monitors team revocation and a configured execution timeout. It confirms local process group termination before releasing the writer lane and retains partial work for review. It cannot promise remote inference cancellation, rollback or containment of processes that escape the owned group.

After a crash or uncertain termination, the attempt remains reserved and reports reconciliation required. Restart does not replay it, rotate accounts, trust a saved PID or automatically free the writer lane. Automatic recovery and additional attempts are intentionally unavailable pending explicit reconciliation. The workspace now exposes explicit agent selection, start and cancellation through these same local APIs. Account setup and activation remain separate operator actions. Follow up editing is not available.

### Trusted verification and local application

`GET /api/projects/{id}/agents` returns eligible project profile metadata without account or credential references. Select a profile before creating a task; legacy requests do not gain authority afterward. `GET /api/jobs/{id}/workflow` reports frozen profile scope, the owned attempt and configured verification state. Reading or refreshing never starts work.

Version two configuration may define `verification_commands`, mapping a configured project ID to one trusted argument array. For example, `{"verification_commands":{"sample-api":["python3","-B","-m","unittest","discover"]}}` runs that configured check. The browser and provider cannot supply commands. These commands execute on the local Mac in the verified isolated worktree with a filtered environment; they are not a security sandbox. Configure them only for trusted projects and dependencies. Python isolated mode ignores environment bytecode settings, so include `-B` when that check must not create cache files.

The workspace displays the exact configured command before Run trusted check. Its request binds the current command authorization hash, and admission is recorded before execution. Duplicate or uncertain requests do not run that command again. A deliberately corrected configuration can authorize a new check while retaining previous evidence, but any earlier unfinished check blocks another command. Successful evidence is bound to the changed text inventory before and after execution and to the actual review scan. Missing, failed, changed or incomplete evidence cannot authorize a native task's review readiness. A passing command proves only what its checks cover.

Scan changes for review records readiness separately from verification. The existing frozen diff and approval controls then bind the exact snapshot, ID and expiry. Approve records permission only. Apply approved patch locally is a separate explicit action that uses the existing live content and clean target checks. A lost acknowledgement reads stored state; an uncertain application is never resubmitted automatically. Local application does not commit, push or publish code.

`POST /api/jobs/{id}/plan` records a deterministic demo planning preview through the durable job and event lifecycle. `GET /api/jobs/{id}/plan` returns the same plan block after a restart, and the event stream can replay it. The preview does not inspect repository files, create a worktree or authorize execution. Native provider planning remains a separate integration step.

`POST /api/jobs/{id}/worktree` creates one isolated Git worktree from the immutable repository snapshot after planning. The service uses a deterministic branch and runtime key, records the binding with its durable event, and leaves the configured project working tree unchanged. Local filesystem paths remain private. Retrying after a restart rebinds a complete, verified and unchanged worktree left by an interrupted database binding. Partial, modified or mismatched artifacts fail closed for manual inspection.

## Status

Foundation implementation is in progress. The first usable vertical slice remains one project, one native provider, one worktree, one diff and one approval. Internal planning, mockups, benchmarks and progress tracking are intentionally maintained outside this public repository.

Current foundation includes the runtime home boundary, initial local directories, versioned public config, strict consultant response validation, `/api/health`, `/api/bootstrap` and durable job submission through `POST /api/jobs`. The durable state layer now includes embedded sequential SQLite migrations plus project, job, append only event, approval, planner, observed usage and immutable `projmem` reference persistence with explicit identity, policy, runtime, request snapshot, payload integrity, scoped idempotency, expiry, an enforced job lifecycle, atomic job state and event recording, atomic planner promotion, truthful nullable quota state and restart recovery classification. SQLite stores only memory identifiers and operational provenance; semantic memory content remains owned by `projmem`.

## Development quick start

The browser workspace is available at `http://127.0.0.1:8765/` after starting the server below. It provides project navigation, task creation, explicitly labeled demo planning, isolated worktrees, native execution controls, configured verification, live events, frozen review and approved local application. Runtime selection alone records a preference; execution requires a project agent selected before submission and a separate Start action. Follow up chat and provider quota are not available.

Selection is restored from project and task identifiers in the URL. Reload replays the durable ledger, while drafts remain only in the open page and are never written to browser storage. Task responses omit internal snapshots and absolute storage paths. The entire HTTP surface requires a loopback client and host, rejects foreign origins and cross site browser requests, and disables response caching. This is a single operator local application, not a multiuser authentication boundary.

`GET /api/jobs?project_id=...` returns recent task summaries in pages of up to 200 entries. Pass the returned `next_cursor` as `before_id` to read an older page. `GET /api/jobs/{id}` returns the selected task with its original request and current state. Both views are restricted to currently configured projects.

The worktree indicator describes the stored task binding, not a fresh filesystem check. Rejected task history remains available after explicit retention cleanup; its recorded binding is preserved even when the artifact has been removed.

### Operations view

The workspace separates project navigation, the focused task and an operations panel. Phone sized screens switch between these panels without discarding the draft. Tasks needing attention appear first within loaded history.

Projects group their loaded task history and offer search and an attention filter. Activity, Changes and Checks share one selected task. Changes retains the frozen review; Checks labels evidence in that snapshot separately from the current verification workflow. Switching views never starts an execution.

`GET /api/projects/{id}/activity` returns bounded project attention and actual recorded native attempts, including the frozen profile identity, cancellation request and reconciliation state. Totals cover stored records even when a list is truncated. The timestamp describes the last job event, not a provider heartbeat. Configured team members appear separately. The current supervisor has one native execution lane.

Project settings reads a safe connection and model summary through `GET /api/projects/{id}/settings`. `POST /api/projects/{id}/team/members` accepts only the stored revision, member IDs and team enablement. It preserves other policy fields and rejects stale revisions. Unavailable stored members can be removed without silently substituting a profile. Lead reassignment remains in runtime configuration. Removing members or disabling a team requests cancellation of affected owned work; it does not claim that a process has stopped. Failed saves retain the draft, and uncertain outcomes are read back without automatic repetition.

Settings selects from the configured profile library. It does not edit shared profiles, add providers, discover models, check authentication or activate paid API execution. Auto Team execution and unsupported model effort controls remain unavailable. Native subscription sources retain their billing class; quota is not inferred.

`GET /api/control?project_id=...` counts every stored task in the selected configured project, independently of history pagination. It returns aggregate lifecycle and requested runtime counts without requests, model names, account identities, file paths or snapshots. These are recorded states, not process liveness or proof of native execution. Provider quota remains explicitly unknown. Failed reads clear the displayed counts rather than leaving an apparently current value.

The review panel shows changed files, recorded verification, warnings and the unified diff. Prepare a snapshot explicitly after review readiness is recorded, view its diff, then prepare a decision. Approval ID, exact snapshot hash, project scope and original expiry appear before Approve locally, Request changes or Reject becomes available. Approval records permission only; this panel never applies a patch or publishes code. Recorded verification may predate the frozen snapshot and is not a new test run.

`GET /api/jobs/{id}/approval` reads durable authority without creating an approval or extending its deadline. Reload recovers pending, expired and decided records. Historical rounds remain visible but cannot authorize a newer round. An uncertain write is not retried automatically: the panel reads stored state and keeps decisions locked until it can establish the outcome. A conflict keeps that snapshot locked. Diff content, file names and command arguments are displayed as text, not interpreted as markup or executable instructions.

Requires Python 3.12 and `uv`:

```bash
uv sync --frozen
AGENT_WORKBENCH_HOME=../working-local uv run uvicorn local_agent_workbench.app:app --host 127.0.0.1 --port 8765
```

Run the complete current quality gate with Node.js 20 or newer available for the dependency free browser controller tests:

```bash
uv run python scripts/repo_boundary.py .
uv run ruff check .
uv run ruff format --check .
uv run pytest
node --test tests/workspace_client.test.cjs
```

## Optional Telegram status reader

The separate status reader uses outbound long polling. It does not expose the dashboard, open an inbound port, install Hermes or invoke an AI provider. Create a dedicated bot through Telegram's official BotFather and obtain your numeric Telegram user ID. Do not share the token in chat or commit it to configuration.

After starting the local backend, run this command with your real user ID and explicitly selected project IDs:

```bash
AGENT_WORKBENCH_HOME=../working-local uv run python -m local_agent_workbench.telegram_gateway --owner-id 123456789 --project sample-api --allow-metadata-egress
```

The placeholder ID is not a working configuration. Enter the bot token at the hidden terminal prompt. It remains in process memory and is not saved or passed through command arguments. The reader accepts `/start`, `/help` and `/status` only from that user in their matching private chat. It ignores other users, groups, forwarded messages, edited messages and commands older than two minutes. Stop it with Ctrl+C.

Enabling this command explicitly permits aggregate counts for the selected public or private projects to leave the Mac for Telegram. Replies use numbered project aliases, not names, task content, diffs, credentials or account details. Internal and restricted project counts are withheld. Telegram bot messages are cloud messages, not Secret Chats, so this is not an end to end encrypted transport.

The command verifies the bot's stable identity and stores receipt hashes, fixed expiry, scope and reply state in the same private runtime database. It records disposition before advancing the polling cursor. Exact duplicates survive restart; changed content under one update ID fails closed. Credentials and incoming message text are not stored. A new process polls from zero and deduplicates against receipts; after prolonged inactivity the cursor also resets because Telegram IDs are not permanently monotonic.

Each reply freezes only the project scopes whose counts it contains and rechecks their current egress policy before dispatch. A durable send claim permits at most one automatic attempt. A lost or malformed acknowledgement, crash after claim or unconfirmed database acknowledgement retains uncertainty and is never automatically resent. Content free help and withheld notices remain available for local only projects. This favors avoiding duplicates over guaranteed delivery; reconciliation is manual. SQLite requests stronger synchronization for journal writes, but process restart tests do not establish hardware power loss guarantees.

This pilot cannot create, run, stop, resume or approve tasks. The Mac must be awake and both the backend and reader must be running. Credential storage, unattended startup and remote task actions remain separate work. Do not share the bot with another poller or webhook consumer. The Python class retains an optional memory only mode for embedded read only use; the command above always uses durable storage.

## Provider capabilities

`ProviderCapabilities` describes a concrete runtime's support for planning, tools, files, streaming and cost reporting. Each capability is explicitly supported, unsupported or unknown; omitted declarations remain unknown. Call `require()` with the task's required capabilities before invoking an adapter. Unsupported or unknown requirements raise `ProviderCapabilityError` with the missing capabilities.

Declarations are immutable snapshots supplied by adapters. This contract does not assign capabilities to real providers, check authentication or health, grant file permissions, or determine prices and remaining quota. Those checks remain separate routing responsibilities.

`ProviderAdapter` defines asynchronous `start`, `send`, `cancel`, `health` and `resume` operations using shared request, session and health types. Adapters must validate capabilities before starting work, reject mismatched sessions, sanitize provider errors and report unsupported operations explicitly. Cancellation acknowledgement does not imply completion, and resume reconnects an existing session without creating a replacement job. Concrete provider connections remain a separate implementation step.

`ProviderHealth` reports one observed provider state as available, unavailable, rate limited or degraded. Every observation carries a timezone aware `observed_at` value and may include a provider supplied future `reset_at` value. The contract does not contain quota percentages or infer values that a provider did not report.

`RuntimeEvent` normalizes text, plan, tool, file, usage, question, error and completion output. Its envelope requires a job identity, positive sequence, concrete runtime and timezone aware timestamp, serialized in UTC. Payload fields are specific to each event kind and reject unknown fields. Usage preserves unknown values as null and represents dollar costs as decimal strings. Tool events carry call identity, name and status; file events describe changes without authorizing file access. Provider adapters remain responsible for sanitizing content before emitting events.

The schema validates individual events. Stream ordering, duplicate handling and persistence remain separate responsibilities; runtime events do not replace the SQLite event ledger.

`RuntimeEventStreamNormalizer` consumes UTF-8 JSON Lines incrementally and assigns trusted job identity, runtime, sequence and observation time. Provider messages contain only `kind` and `payload`. Chunk boundaries, multibyte text, blank lines and a final line without a newline are handled without losing order. Invalid encoding, JSON, duplicate fields, oversized messages and invalid event payloads produce sanitized error events without exposing raw provider output or preventing later valid messages from being processed.

The normalizer does not persist events, change durable job state, authorize provider activity or interpret a provider native protocol. Each native adapter remains responsible for translating its own output into the small JSON Lines fragment contract before normalization.

`FakeProvider` supplies a deterministic demo through the adapter protocol. Its `events(session)` method returns an immutable snapshot containing a simulated plan, write proposal, verification message and review question. Sending `approve` or `reject` finishes the demo; cancellation is repeatable. Events use a fixed synthetic timestamp and clearly label simulated outcomes. The adapter does not execute tools, modify files, grant approvals or resume sessions after restart.

The reusable provider conformance suite applies the same capability, health, identity, messaging, cancellation and resume assertions to every registered adapter. `FakeProvider` and the Codex native adapter inherit the same suite.

## Codex native runtime

`CodexAdapter` implements the shared `StreamingProviderAdapter` contract and executes the official Codex CLI through an existing ChatGPT subscription. The initial request is written through standard input, so private task text is not placed in the process argument list. Each run uses JSON Lines output, the `workspace-write` sandbox, disabled command network access and web search, no escalation prompts, no login shell and a filtered child environment. Provider stderr, native command output, account paths and credential material are never copied into normalized events.

Every `CodexAccountSlot` owns an independent `CODEX_HOME` below private runtime storage. The full slot directory is sensitive because it contains authentication and Codex session history. `CodexAccountPool` checks CLI authentication and selects available slots in deterministic round robin order. A selected slot remains bound to its session for follow up messages. The adapter never changes accounts during an active turn and never retries failed file work on another account, which prevents duplicate mutations. Logical slot names contain no email address or account identity.

Provider health reports only observed login availability. It does not claim remaining quota, rate limit state or subscription entitlement that the CLI did not expose. Usage events preserve token counts reported by Codex and leave dollar cost unknown because subscription activity has no per request API price in this adapter.

Native Codex messages are translated into the shared text, plan, tool, file, usage, error and completion contract. File events retain only safe worktree relative paths. Provider failures and malformed messages become fixed sanitized errors. Continued messages use the original Codex thread and account. Resume after an application restart remains explicitly unsupported until the account binding is persisted durably.

`WriteBoundary` anchors every file capable Codex session to one canonical worktree and an immutable tuple of allowed relative directories. Path validation rejects traversal, protected control metadata, existing symlink or hard link aliases, special files, replaced roots and paths outside the allowlist. Each allowed directory must exist before execution. An invalid provider file report ends the turn with sanitized events instead of accepting a partial batch.

The same verified directories become the Codex `workspace-write` roots, while temporary directory writes, command network access, apps, hooks and subagents remain disabled. Application path checks do not treat provider output as proof that an unreported write was safe. Later review and promotion stages must inspect the actual worktree before accepting changes.

Account rotation is an availability and separation feature for accounts the operator is authorized to use. It is not a quota bypass mechanism, and operators remain responsible for the applicable service terms.

## Read only review diff

`ReadOnlyDiffService` compares an isolated worktree with its immutable snapshot commit and returns a deterministic inventory of staged, unstaged, deleted, renamed and untracked files. Collection does not change `HEAD`, the index or worktree content. Repository hooks, external diff programs, text conversion programs and configured content filters cannot execute during inspection.

Every path passes the canonical worktree boundary before content is read. Symlinks, hard links, special files, submodules, malformed paths and changing repository state fail closed with sanitized errors. Binary and oversized content is represented only by file status and byte counts. Text patches are included only as complete UTF 8 unified diffs within explicit file count, file size, context and total output limits.

The service reads raw worktree content instead of trusting repository supplied conversion programs. It verifies `HEAD`, Git configuration, changed file inventory and file identity again before returning a result. The API route and review interface consume this service in later vertical slice tasks.

`VerificationRunner` executes a configured focused command inside a job worktree only while the job is in the verifying state. Commands use structured arguments without shell parsing, a filtered child environment and a finite timeout. Each completed pass, failure or timeout becomes immutable SQLite evidence containing the exact arguments, exit code when available, elapsed milliseconds, output byte counts and a framed SHA 256 digest. Raw stdout and stderr are never written to the database. Cancellation stops the process group without recording a misleading result.

`POST /api/jobs/{id}/review-readiness` collects the bounded worktree diff and scans only added text before a job can enter `review_ready`. Known provider credentials, private keys, bearer tokens, credential URLs and explicit credential assignments block the transition. Binary, oversized and omitted additions also fail closed because their contents were not fully inspected. Findings contain only a rule identifier, repository relative path and line number. Matched values never enter responses, events or logs. A passing scan and state transition commit together, while a blocked scan records a sanitized event and leaves the job in `verifying`.

`POST /api/jobs/{id}/review-bundle` freezes one snapshot per review round after the secret gate has approved the exact current diff. `GET /api/jobs/{id}/review-bundle` reads the latest stored snapshot without rebuilding it. The bundle contains the task summary, bounded review diff, recorded verification commands and outcomes, explicit risk notes, runtime and model, the secret scan event identifier, and content hashes. Command output is represented by its digest and byte counts rather than raw text. A duplicate creation request returns the same bundle only when the reviewed content and evidence still match. Stored snapshots cannot be updated or deleted, and reads verify each canonical SHA 256 hash. The response is not cached. This endpoint does not approve or apply changes.

After displaying a bundle, submit its `bundle_hash` to `POST /api/jobs/{id}/approval-request`. The server checks the live worktree again and atomically records a pending approval, its event and the `waiting_approval` job state. The response supplies an approval identifier and a 15 minute expiry. Submit `{"decision":"approved","bundle_hash":"..."}`, with `approved`, `rejected` or `changes_requested`, to `POST /api/approvals/{id}`. The same service handles every decision: it checks the frozen bundle against the live diff, then atomically records one decision event and the corresponding job state. Exact retries return the original result; conflicting retries and expired approvals fail closed. Approval endpoints require a loopback client and local host, reject foreign browser origins and return uncached responses. The current local API records the actor as `local-user`; this is a local operator label, not verified personal identity. A changes request returns the job to `running`; the next review creates a new immutable bundle while retaining prior rounds. Approval never applies, commits or publishes the worktree. Promotion is a separate gate and must recheck the approved hash immediately before applying. An expired pending approval requires operator recovery before a new request.

`CancellationService` validates provider and durable job identity before requesting cancellation. Acknowledgement alone does not change durable state. Only a matching normalized completion event with cancelled status permits one atomic terminal transition through a job scoped idempotency key. Retries return the committed event, conflicts fail closed, and cancellation preserves the recorded worktree for later review and cleanup policy.

`GET /api/jobs/{id}/events` replays the durable job event ledger and follows newly persisted events as Server Sent Events. Each data frame carries the stable database event identifier, job sequence, event type, payload and persistence timestamp. Clients reconnect with `Last-Event-ID`; the server resumes after the matching job event without a gap or duplicate. Idle connections receive comment heartbeats that never advance the cursor. Invalid cross job cursors fail before streaming begins, and storage failures close the connection with a sanitized control event.

## Process execution

`run_process()` executes a tuple of arguments without shell parsing, closes stdin and captures stdout and stderr separately. It returns nonzero exit codes as results. Timeout and task cancellation kill the POSIX process group and reap the direct child before returning control. Children that deliberately create another session are outside that process group.

This runner targets macOS and Linux commands with bounded output; captured output is held in memory. It is not a sandbox, a command authorization policy or a continuous streaming interface. Callers must select permitted executables and treat captured output as potentially sensitive.

## Database backup

Create a consistent snapshot of the existing runtime database while the application is running:

```bash
AGENT_WORKBENCH_HOME=../working-local uv run agent-workbench backup ../working-local/state-backup.db
```

The destination parent must already exist. Each backup requires a new filename outside source repositories. Existing files are never replaced. The command checks database integrity before publishing the snapshot and exits unsuccessfully if verification fails. It does not initialize or migrate the source database.

Backups contain private application data. Store them in a protected local directory. The snapshot covers SQLite records only; configuration, credentials, worktree files and external `projmem` content require separate backups. Recovery must be tested in an isolated runtime before replacing live data.

## Local promotion

After approval, submit `{"bundle_hash":"..."}` to `POST /api/approvals/{id}/promote` from a loopback client. The service rechecks the approved worktree and frozen text diff, verifies that the original project checkout is clean and at the submitted commit, then applies the reviewed patch locally. It records `applying` before any target write and `completed` only after the changed path set and final file contents match the approved worktree. An interrupted or partial application remains `applying` for manual reconciliation and is never retried automatically. Promotion does not stage, commit, push or deploy.

## Rejected worktree retention

Rejecting a review keeps its worktree and atomically records an inspection deadline. The default is seven days, configured through `retention.rejected_worktree_days` in `config.json` (whole days from 1 to 365). Each rejection freezes its deadline, so later configuration changes and retries cannot shorten or extend it. Deadlines use the trusted local UTC clock, not a guaranteed physical elapsed duration across clock adjustments.

Read `GET /api/jobs/{id}/retention` from a loopback client before disposal. After the deadline, an explicit `POST /api/jobs/{id}/cleanup` with `{"retention_event_id":123}` identifies the displayed record. The service verifies the original linked worktree identity, registration, creation evidence and unchanged rejected review. Ignored files, nested repositories, locked trees and unsafe paths block disposal. The project lock coordinates workbench operations but cannot exclude external filesystem writers. Do not edit the retained worktree during cleanup. Startup and rejection never perform cleanup automatically. Older rejected jobs without a retention record remain available for manual inspection.

Cleanup records its intent before removing the exact worktree, then records completion only after verifying that its directory and registration disappeared and its branch remained. Job history, approvals and frozen review bundles remain in SQLite. The [Git worktree removal command](https://git-scm.com/docs/git-worktree) discards the reviewed uncommitted contents, which are not recoverable from the preserved branch. Retain a separate backup before cleanup when those contents are still needed. An interrupted operation reports `needs_attention` and cannot repeat deletion. Reconcile it manually; there is no recursive deletion or metadata pruning fallback.

## Cost boundary

1. Claude Code and Codex: user's existing native subscriptions.

2. Local Qwen: local compute with no API charge per token.

3. DeepSeek consultant: user's OpenRouter key, optional and budget capped.

4. Community users bring their own subscriptions and keys; the project never bundles credentials.

License: MIT.

## V1 boundaries

V1 deliberately does not build an editor, team SaaS, autonomous deployer, scraper or fine tuning pipeline. It opens files in the user's IDE, uses official GitHub and IMAP integrations, and keeps fine tuning and quantization for the evidence based phase after real usage exists.
