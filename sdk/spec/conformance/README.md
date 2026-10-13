# Whitewater SDK conformance corpus

Each `*.json` file is a named scenario every SDK's conformance runner
executes against a real three-Node Riverbed. A runner that cannot execute
a listed operation must report it explicitly (`unsupported`), never skip
silently — the parity claim is only as strong as the weakest SDK.

## Fixture vocabulary

```json
{
  "scenario": "name",
  "api_key": "optional scenario-level override of the admin key",
  "requires": ["ops", "the", "sdk", "must", "support"],
  "steps": [ ... ]
}
```

`api_key` replaces the environment key for every step in the scenario —
use it to assert that a wrong key is refused rather than to authenticate
successfully.

### Step kinds

| `op` | Maps to | `args` |
|---|---|---|
| `wcl` | `admin.wcl` | `script` |
| `admin.commands` | `admin.commands` | `commands` (typed command objects) |
| `writer.append` | `writer.append` | `writer`, `session_epoch` (from a prior `bind`), `key`, `payload`, `request_id`, `event_time_ns` |
| `writer.append_batch` | `writer.append_batch` | `records: [...]` same fields |
| `reader.open` / `reader.fetch` / `reader.ack` / `reader.close` | reader ops | `reader`, `session_epoch`/`cursor`/`limit`/`capacity` |
| `reader.temporary_fetch` | temporary fetch | `feed`, `after`, `limit`, `new_only`, `tail` |
| `member.join` / `member.claim` / `member.renew` / `member.release` / `member.fetch` / `member.ack` | subscription member ops | `subscription`, `member_id`, `member_epoch`, `work_id`, `lease_epoch`, `limit`, `lease_ticks` |
| `feed.append` | `feed.append` | `feed`, `request_id`, `writer_session_id`, `writer_epoch`, `sequence`, `key`, `payload`, `event_time_ns` |
| `feed.records` | `feed.records` | `feed`, `limit`, `after` |
| `expect` | assertions | see below |

### Bindings and substitution

- `{{uniq}}` — a run-unique lowercase identifier; the runner generates one
  per execution so parallel runs never collide.
- `{{uuid}}` — a fresh random UUID per occurrence.
- `{{uuid:name}}` — a run-unique UUID named `name`; every occurrence in the
  run substitutes the same generated value (use it for `request_id`,
  `member_id`, `work_id` that a later step must repeat).
- `"bind": "name"` on a step stores its response under `name`.
- `"{{name.field}}"` in a later step's args substitutes that field's value.
  Nested access: `{{join.member_epoch}}`, `{{fetch.cursor}}`.
- `raw` args (`"key": {"utf8": "orders-42"}`, `"key": {"hex": "00ff"}`)
  decode before the call.

### `expect` assertions

Each entry is `{ "path.to.field": expected }` over the named bound result
(or the immediately previous step when unnamed). Operators:

- literal value — deep equality
- `"==<bind>.<field>"` — equals another bound field
- `"!=null"` — present and non-null
- `"present"` / `"absent"`
- `"len>=N"` — array/string length bound
- `{ "error_code": "..." }` — the step must have failed with this wire
  `code` (e.g. `writer.stale_sequence`, `lease.stale_lease`)
- `{ "error_status": 401 }` — the step must have failed with this HTTP
  status; use it for failures that carry no wire `code` (authentication
  refusals, generic sequence conflicts). The two operators may be
  combined when both are meaningful.

Scenarios run against a fresh Riverbed with the development admin key from
`WHITEWATER_ADMIN_API_KEY` (default `whitewater-local-development-admin-key`).
A runner creates its own Domain via `wcl` before use — never hardcode a
pre-existing resource.
