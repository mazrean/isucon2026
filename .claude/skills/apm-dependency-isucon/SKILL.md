---
name: apm-dependency-isucon
description: Lists the CLI tools that must appear in mise.toml for any repo depending on apm-package/isucon. Use when bootstrapping or auditing an ISUCON contest workspace's mise.toml.
---

# ISUCON package — mise dependencies

```toml
[tools]
# Required by the `serena` MCP server registered by this package.
# Serena is launched via `uvx --from git+https://github.com/oraios/serena ...`.
"aqua:astral-sh/uv" = "latest"
```

## Notes

- `uv` is required for `uvx`, which is how this package starts the
  `serena` MCP server. Do **not** pin Serena itself in `mise.toml` — uvx
  pulls the latest published build per session.
- The `grafana` MCP server is a Docker container provisioned by
  `mazrean/isucon-ansible/roles/monitor_local`. It is **not** a CLI and
  does not appear in `mise.toml`. Run `make monitor` (or
  `ansible-playbook monitor.yaml`) once before starting the agent so the
  container is reachable at `http://localhost:8000/mcp`.
- ISUCON repos that pick Go for the application SHOULD additionally
  depend on `apm-package/go` (which pins `go`, `gopls`, and registers
  the `gopls` MCP server). Do not duplicate those pins here.
- Ansible itself is intentionally NOT pinned via mise — install it per
  developer (system package or `pipx install ansible`).
- The coding-agent CLI (`claude-code`, `codex`, `gemini-cli`, etc.) is
  intentionally NOT pinned, matching `apm-package/common`'s policy.
