---
paths:
  - "**"
---

# Project conventions

## Agent instruction file

- `AGENTS.md` is the single source of truth.
- `CLAUDE.md` MUST contain only `@AGENTS.md` so Claude Code imports the same content.
- Other client-specific instruction files (`GEMINI.md`, `.cursor/rules`, etc.) should also point at `AGENTS.md` rather than duplicate content.

## Spec-driven development

- New specs live under `specs/`. The legacy `.kiro/specs/` layout is deprecated.
- Use the `writing-feature-spec`, `writing-technical-design`, `writing-implementation-tasks`, and `writing-project-constitution` skills from `mazrean/agent-skills`.
- `cc-sdd` and `github/spec-kit` are deprecated org-wide.

## Commits

- Use Conventional Commits via the `committing-code` skill.
- Keep commits atomic; never bypass hooks (`--no-verify`) without explicit authorization.

## Tool versions

- Pin CLI tool versions in `mise.toml` at the repo root.
- The coding-agent CLI (claude-code / codex / etc.) is NOT pinned in mise — install per developer.
- Each `apm-package` stack package ships an `apm-dependency-<package>` Agent Skill listing the tools it expects in `mise.toml`. The repo owner hand-merges these into the local `mise.toml`.

## Command output (rtk)

- [`rtk-ai/rtk`](https://github.com/rtk-ai/rtk) compresses command output before it reaches the context window. It is expected in `mise.toml` (see the `apm-dependency-common` skill).
- The `rtk-rewrite` hook shipped with this package registers a Claude Code `PreToolUse` (`Bash`) hook running `rtk hook claude`, so plain commands are rewritten to their rtk equivalent automatically. Do NOT run `rtk init` inside a repo — project-local init rewrites `CLAUDE.md`, which must stay `@AGENTS.md` only. `rtk init -g` is not needed either; rewriting is idempotent, so a pre-existing global hook is harmless.
- Treat compressed output as the complete result. Re-run as `rtk proxy <cmd>` only when a result is unusable (empty where output was expected, contradicting its exit code, or garbled); `rtk recall` restores output a filter elided.
- `rtk gain` shows token savings.
- The shipped hook carries the Claude processor (`rtk hook claude`). `apm install --target copilot` deploys the same command to `.github/hooks/`, which Copilot cannot read — non-Claude clients should install with `--target claude` and register their own hook via `rtk init -g --copilot` / `--gemini` / `--codex`.

## Browser automation

- Use the Playwright CLI + the Playwright Agent Skill. Do NOT register a Playwright MCP server.

## Adding new shared skills

- Reusable skills (cross-repo) belong in `mazrean/agent-skills`, not in individual repos.
- Per-stack standards belong in the appropriate `mazrean/apm-package/<stack>` package.
