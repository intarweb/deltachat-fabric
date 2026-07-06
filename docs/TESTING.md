# Testing — units, integration seams, and the live-only surface

The fix that shipped in PR #3 (startup freeze + no account onboarding) was a lesson: **the
unit tests were green while the app wouldn't boot.** The functions were tested; the
*integration seams* (does the app start, do the uvicorns bind, does the reconciler actually
onboard into the deltachat core, does the event stream parse a real event) were not — and
that is exactly where both bugs lived.

This file records what the suite covers, the regressions that guard the seams, and — most
importantly — the boundaries that are **genuinely only provable against a live deltachat
core / chatmail server**, so we know the deploy-time live-verify is load-bearing there.

## Principle

> **Test the seams, not just the funcs.** A green unit suite means the logic is right; it
> does NOT mean the app boots, binds, or talks to its dependencies. For every integration
> boundary, either add a test that exercises the *real* wiring (even with a faked dependency
> at the edge), or document it as live-only and make the deploy checklist cover it.

## Covered by the offline suite (`pytest -q`, no network / no rpc-server)

- **Pure logic:** routing (anti-thundering-herd), reconcile diff, password minting, config
  parsing, hold-queue idempotency, backup scheduling/rotation, mention extraction.
- **HTTP contract:** every relay endpoint (`/send`, `/contacts`, `/channels`, `/send_channel`,
  `/channel`, `/channel/member`, `/react`, `/healthz`) via `TestClient` with a fake backend.
- **MCP tools:** all 7 tools register with clean schemas and delegate to the right relay
  method/path/body (`httpx.MockTransport`); `/mcp` is mounted.
- **Integration regressions (guard the two bugs that shipped):**
  - `test_serve_boots_and_both_uvicorns_bind_with_blocking_backend` — boots the **real**
    `main._serve` gather on ephemeral ports with a backend whose event read *blocks* like the
    real `get_next_event`; asserts both uvicorns bind + respond. **Fails (times out) if the
    blocking read regresses onto the event loop** (the freeze).
  - `test_event_pump_dispatches_incoming_and_never_blocks_the_loop` — the pump consumes a
    blocking stream in a thread and bridges incoming msgs onto the loop without freezing it.
  - `test_incoming_ids_selects_by_type_not_kind_string` — guards the message-drop bug:
    `EventTypeIncomingMsg` is selected by **type**, not a (non-existent) `.kind` string.
  - `test_ensure_account_onboards_into_the_core_not_just_imap` + idempotency — onboarding
    calls `add_account` + `add_or_update_transport` (real deltachat2 `EnteredLoginParam`/
    `Socket` types via a fake rpc), not merely an IMAP login.
  - **Host-header / DNS-rebinding** — the boot test POSTs a real `initialize` to `/mcp` with a
    non-localhost `Host: mcp-deltachat:8000` and asserts it is **not 421-rejected** (the bug
    that blocked an in-cluster gateway); `test_transport_security_*` guard the setting.

deltachat2 API usage is verified against the **installed package** (not just docs):
`EnteredLoginParam`/`Socket` fields, `Rpc.{add_account,add_or_update_transport,is_configured,
start_io,get_next_event,...}` signatures, and `Event.context_id` / `EventTypeIncomingMsg`
shape.

## Live-only seams — NOT unit-testable; the deploy live-verify is load-bearing

These touch a real `deltachat-rpc-server` core and/or a live chatmail (Dovecot/SMTP) server.
They're isolated behind the `DeltaBackend` seam with `# pragma: no cover` + defensive
`getattr`, so an API drift is a one-place fix — but they can only be *proven* on deploy.

| Seam | Why not unit-testable | How de-risked | Live-verify (deploy checklist) |
|---|---|---|---|
| `DeltaChat2Backend.__init__` (IOTransport/Rpc spawn + `start_io_for_all_accounts`) | needs the `deltachat-rpc-server` binary + a real accounts dir | injectable `_rpc`/`_io_transport` seam; construction unit-tested with a fake rpc | container starts; logs `event pump thread started` |
| **Account onboarding vs a live chatmail** (`add_or_update_transport` = create-on-login + configure) | needs a live Dovecot/SMTP that create-on-logins + a real keygen | orchestration + arg-shaping unit-tested (fake rpc); signatures package-verified | `/data/accounts` gets per-bot `.db`; log `reconcile: N/N account(s) onboarded` |
| **Inbound event parsing** (`_build_inbound`: `get_message`/`get_basic_chat_info`/`get_chat_contacts`/`get_contact`) | real event/message/chat snapshot field names are version-fragile across cores | type-based selection (`incoming_ids`) unit-tested; body build behind the seam | a real inbound group msg wakes the right bot(s) |
| **Message delivery** (`send` → `rpc.send_msg`) | needs a configured account + live SMTP | `/send` HTTP contract + routing unit-tested with a fake backend | a real `delta_send` / `delta_list_channels` returns live data |
| **contacts/channels/react** enumeration (`get_contacts`/`get_chatlist_entries`/`send_reaction`) | JSON-RPC names could not be signature-verified from reachable autodocs | isolated behind the seam with defensive fallbacks | exercise each MCP tool against the live account |
| **Nightly imex backup** (`export_backup`) | needs a live account DB | scheduling/rotation unit-tested; the one imex call is `# pragma: no cover` | a backup tar appears in `/backup` after the interval |
| **MCP end-to-end** (gateway → `/mcp` → relay HTTP → backend send) | full chain needs a live core + a real MCP client handshake | each hop unit-tested (tools, delegation, relay HTTP); `/mcp` bind covered by the boot test | gateway `tools/list` + a real tool call succeed |

**Deploy live-verify is therefore required** for every "live-only" row above — a green unit
suite is necessary but not sufficient. See the redeploy checklist (both bars: bind + `/mcp`
**and** real `.db` onboarding + a live send).
