# Local Agent Workbench

> A local AI workbench where Claude, Codex and local Qwen work together with durable state, while an optional affordable consultant provides bounded advice.

Local Agent Workbench is an open source macOS application for running real project work through multiple AI runtimes from one persistent app. It is not another chat wrapper: every task has durable state, an isolated worktree, an explainable route, a review bundle and an explicit promotion gate.

## Product in one minute

The app opens at login and exposes two native windows:

1. **AI Workspace:** project chat, `Auto / Claude / Codex / Local`, plan, tool stream, files, diff, tests, approvals and provider health.

2. **Planner:** calendar, deadlines, job queue, `Needs you`, watcher inbox, notifications and the audit ledger.

The default flow is:

```text
task → local policy check → plan → isolated worktree → execute
     → verify → review → human approval → apply locally → remember
```

## What makes it different

1. Native Claude Code and Codex runtimes use the user's subscriptions; this project does not turn them into paid API calls.

2. OpenRouter is optional and restricted to `deepseek/deepseek-v4-flash-0731` as an untrusted consultant. It cannot execute tools, write files or approve actions.

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

## Project configuration

Copy the public example into the runtime home, then set each project root to an existing Git worktree:

```bash
cp config.example.json ../working-local/config.json
```

Relative project roots are resolved from the directory containing `config.json`. Each root must identify the top level directory of a Git worktree. Missing paths, regular files, non Git directories and nested worktree paths stop startup with a safe configuration error.

The application can start without `config.json` and reports an empty project list. When configuration is present, matching projects are registered once in SQLite. A later configuration that conflicts with persisted project identity or policy fails closed instead of rewriting durable state.

## Status

Foundation implementation is in progress. The first usable vertical slice remains one project, one native provider, one worktree, one diff and one approval. Internal planning, mockups, benchmarks and progress tracking are intentionally maintained outside this public repository.

Current foundation includes the runtime home boundary, initial local directories, versioned public config, strict consultant response validation and `/api/health` plus `/api/bootstrap`. The durable state layer now includes embedded sequential SQLite migrations plus project, job, append only event, approval, planner, observed usage and immutable `projmem` reference persistence with explicit identity, policy, runtime, request snapshot, payload integrity, scoped idempotency, expiry, an enforced job lifecycle, atomic job state and event recording, atomic planner promotion, truthful nullable quota state and restart recovery classification. SQLite stores only memory identifiers and operational provenance; semantic memory content remains owned by `projmem`.

## Development quick start

Requires Python 3.12 and `uv`:

```bash
uv sync --frozen
AGENT_WORKBENCH_HOME=../working-local uv run uvicorn app:app --host 127.0.0.1 --port 8765
```

Run the complete current quality gate:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

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

The reusable provider conformance suite applies the same capability, health, identity, messaging, cancellation and resume assertions to every registered adapter. `FakeProvider` is the current deterministic subject. A native adapter must inherit the same suite and pass its assertions before the provider contract gate can close.

`CancellationService` validates provider and durable job identity before requesting cancellation. Acknowledgement alone does not change durable state. Only a matching normalized completion event with cancelled status permits one atomic terminal transition through a job scoped idempotency key. Retries return the committed event, conflicts fail closed, and cancellation preserves the recorded worktree for later review and cleanup policy.

`GET /api/jobs/{id}/events` replays the durable job event ledger and follows newly persisted events as Server Sent Events. Each data frame carries the stable database event identifier, job sequence, event type, payload and persistence timestamp. Clients reconnect with `Last-Event-ID`; the server resumes after the matching job event without a gap or duplicate. Idle connections receive comment heartbeats that never advance the cursor. Invalid cross job cursors fail before streaming begins, and storage failures close the connection with a sanitized control event.

## Process execution

`run_process()` executes a tuple of arguments without shell parsing, closes stdin and captures stdout and stderr separately. It returns nonzero exit codes as results. Timeout and task cancellation kill the POSIX process group and reap the direct child before returning control. Children that deliberately create another session are outside that process group.

This runner targets macOS and Linux commands with bounded output; captured output is held in memory. It is not a sandbox, a command authorization policy or a continuous streaming interface. Callers must select permitted executables and treat captured output as potentially sensitive.

## Database backup

Create a consistent snapshot of the existing runtime database while the application is running:

```bash
AGENT_WORKBENCH_HOME=../working-local uv run python app.py backup ../working-local/state-backup.db
```

The destination parent must already exist. Each backup requires a new filename outside source repositories. Existing files are never replaced. The command checks database integrity before publishing the snapshot and exits unsuccessfully if verification fails. It does not initialize or migrate the source database.

Backups contain private application data. Store them in a protected local directory. The snapshot covers SQLite records only; configuration, credentials, worktree files and external `projmem` content require separate backups. Recovery must be tested in an isolated runtime before replacing live data.

## Cost boundary

1. Claude Code and Codex: user's existing native subscriptions.

2. Local Qwen: local compute with no API charge per token.

3. DeepSeek consultant: user's OpenRouter key, optional and budget capped.

4. Community users bring their own subscriptions and keys; the project never bundles credentials.

License: MIT.

## V1 boundaries

V1 deliberately does not build an editor, team SaaS, autonomous deployer, scraper or fine tuning pipeline. It opens files in the user's IDE, uses official GitHub and IMAP integrations, and keeps fine tuning and quantization for the evidence based phase after real usage exists.
