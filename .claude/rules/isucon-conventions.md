---
paths:
  - "**"
---

# ISUCON workspace conventions

## Workflow

The contest workspace follows a fixed order:

1. **Bootstrap the monitoring stack on the operator machine** with the
   `monitor_local` role in `mazrean/isucon-ansible` (`make monitor` /
   `ansible-playbook monitor.yaml`). This brings up Loki / Prometheus /
   Pyroscope / Grafana / Alloy / mcp-grafana via Docker Compose and sets
   up the SSH port forwards into each contest server.
2. **Deploy the application** with `mazrean/isucon-ansible`'s `server.yaml`
   playbook (roles: `common`, `tools`, `repo`, `kernel_param`, `fluentbit`,
   `app`, `mysql`, `nginx`). See the `deploying-with-isucon-ansible` skill.
3. **Operate live servers** with the `operating-isucon-servers-via-ssh`
   skill — never edit production paths by hand without it.
4. **Investigate metrics / logs / profiles** through the `grafana` MCP
   server registered by this package, guided by the `using-grafana-mcp`
   skill.

## MCP servers registered by this package

- `grafana` — talks to the `mcp-grafana` container provisioned by
  `monitor_local`. Reachable at `http://localhost:8000/mcp` (streamable
  HTTP). The container already authenticates to Grafana with the admin
  credentials baked into the Compose file, so no token configuration is
  required on the agent side. If the MCP server is unreachable, the
  monitoring stack has not been bootstrapped — run the monitor playbook
  first.
- `serena` — multi-language symbol-aware navigation MCP, started via
  `uvx`. Treat it as the cross-language equivalent of `gopls mcp` and
  pair it with `gopls` when the contest webapp is Go.

## Planning / architecture

- `winning-isucon` is the top-level playbook for the contest day.
- `designing-isucon-architecture` and `writing-isucon-arch-subagent-tasks`
  drive the planning phase before any code change.
- `porting-isucon-rust-to-go` is only relevant when the chosen reference
  implementation is Rust and the team intends to switch.

## Out of scope here

- Per-language tooling (Go DI, lint, etc.) belongs in `apm-package/go`,
  not here. ISUCON repos that pick Go SHOULD depend on **both**
  `apm-package/isucon` and `apm-package/go`.
- Frontend tooling (Astro / Lit / Vite) belongs in `apm-package/frontend`.
- Setup / authentication of the Grafana MCP server itself is documented
  in `mazrean/isucon-ansible`, not in the `using-grafana-mcp` skill.
