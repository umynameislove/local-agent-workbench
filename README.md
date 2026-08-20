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

## Status

Foundation implementation is in progress. The first usable vertical slice remains one project, one native provider, one worktree, one diff and one approval. Internal planning, mockups, benchmarks and progress tracking are intentionally maintained outside this public repository.

Current foundation includes the runtime home boundary, initial local directories, versioned public config, strict consultant response validation and `/api/health` plus `/api/bootstrap`.

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

## Cost boundary

1. Claude Code and Codex: user's existing native subscriptions.

2. Local Qwen: local compute with no API charge per token.

3. DeepSeek consultant: user's OpenRouter key, optional and budget capped.

4. Community users bring their own subscriptions and keys; the project never bundles credentials.

License: MIT.

## V1 boundaries

V1 deliberately does not build an editor, team SaaS, autonomous deployer, scraper or fine tuning pipeline. It opens files in the user's IDE, uses official GitHub and IMAP integrations, and keeps fine tuning and quantization for the evidence based phase after real usage exists.
