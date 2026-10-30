---
name: apm-dependency-common
description: Lists the CLI tools that must appear in mise.toml for any repo depending on apm-package/common. Use when bootstrapping or auditing a repo's mise.toml.
---

# Common package — mise dependencies

Every repo that depends on `mazrean/apm-package/common` SHOULD include the following tools in its `mise.toml`:

```toml
[tools]
"github:microsoft/apm" = "latest"
"github:rtk-ai/rtk" = "latest"
gh = "latest"
```

## Notes

- `github:microsoft/apm` is the [Agent Package Manager](https://github.com/microsoft/apm), installed via mise's GitHub backend so releases are pulled directly from the upstream repo. Required for `apm install` / `apm compile`.
- `github:rtk-ai/rtk` is [RTK](https://github.com/rtk-ai/rtk), a CLI proxy that compresses command output before it reaches the agent's context. Also via the GitHub backend — rtk is not in the mise registry. This package ships the `rtk-rewrite` hook (`PreToolUse` / `Bash` → `rtk hook claude`), so no `rtk init` is needed per repo; running `rtk init` locally would rewrite `CLAUDE.md`, which must stay `@AGENTS.md` only.
- `gh` is the GitHub CLI. Used by the `committing-code` skill and any PR-related workflow.
- The coding-agent CLI itself (`claude-code`, `codex`, `gemini-cli`, etc.) is intentionally NOT pinned in mise — developers install their preferred client locally.
- This list is hand-merged into each repo's `mise.toml`; `apm` does not generate `mise.toml` automatically.
