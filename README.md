# Delta Chat Fabric (DCF)

A **generic, deployable engine** that lets a fleet of agents talk to each other (and to
humans) over **[Delta Chat](https://delta.chat)** — end-to-end-encrypted email-transport
chat — instead of a bespoke bus. One process manages every bot's Delta account, exposes a
small HTTP contract for sending, and wakes the *right* bot(s) on inbound group messages.

**It is a Bambu-style engine: zero fleet identity is baked into the code or image.** The
domain, roster, mail host, a2a-directory URL, ports, directories, and schedules all inject
at **deploy** via environment variables + a mounted roster file. The published image is
reusable by anyone; your specifics never live in the repo.

## What it is

- **Relay** (`app/relay.py`) — one process owns *all* bot Delta accounts through the
  deltachat account-manager + `deltachat-rpc-server`. It serves an internal HTTP `/send`
  contract and runs an inbound loop that routes each incoming group message to the correct
  bot(s) over **a2a** (agent-to-agent wake).
- **Reconciler** (`app/reconciler.py`) — keeps the set of Delta accounts aligned with the
  desired roster using the **chatmail create-on-login** model: an account is provisioned
  simply by a successful IMAP LOGIN (idempotent). It also computes the *prune* diff
  (accounts no longer desired) — reported only; server-side removal is out of scope.
- **Routing** (`app/routing.py`) — the anti-thundering-herd rule: @mentions wake only the
  mentioned members; an unaddressed message wakes only the channel's "main" (realm lead);
  **never** wakes all members.
- **Backup** (`app/backup.py`) — nightly deltachat **imex** export of each account
  (portable backup, *not* a raw SQLCipher copy) into a backup dir, with per-account
  retention.
- **Service entrypoint** (`app/main.py`) — wires it all and runs the reconciler loop, the
  relay inbound loop, uvicorn (`/send`), and the backup loop concurrently in one asyncio
  process.
- **MCP tools** (`app/mcp_tools.py`) — thin clients over the relay's HTTP contract, so an
  agent can send/manage Delta chats as tool calls.

Everything that touches the outside world (deltachat rpc-server, IMAP, a2a HTTP) sits
behind a thin **injectable** seam, so the whole engine is unit-tested with no live
rpc-server and no network.

## The 8 MCP tools

Each is a thin client over the relay HTTP contract (base URL from `RELAY_URL`):

| Tool | Relay call | Purpose |
|---|---|---|
| `delta_send` | `POST /send` `{bot_id,target,text}` | Send a message as a bot into a chat/contact id |
| `delta_list_contacts` | `GET /contacts?bot_id=` | List a bot account's known contacts |
| `delta_list_channels` | `GET /channels?bot_id=` | List a bot account's group chats (channels) |
| `delta_send_channel` | `POST /send_channel` `{bot_id,channel_id,text}` | Send into a group chat |
| `delta_create_channel` | `POST /channel` `{bot_id,name,members[]}` | Create a group chat with caller-supplied members |
| `delta_add_member` | `POST /channel/member` `{bot_id,channel_id,contact}` | Add one caller-supplied contact to a channel |
| `delta_react` | `POST /react` `{bot_id,chat_id,msg_id,emoji}` | Set an emoji reaction on a message |
| `delta_secure_join` | `POST /secure_join` `{bot_id,invite}` | Accept a securejoin/verified invite (link or QR) → the inviter becomes a verified key-contact (E2E key-exchange), so they can be added to an encrypted channel / messaged E2E. The human-onboarding mechanism. |

`bot_id` also accepts the alias `localpart`. Non-2xx responses surface as errors carrying
the relay's `detail` (404 = unknown bot).

## The relay `/send` contract

```
POST /send
  { "bot_id": "<bot id or localpart>", "target": <chat_or_contact_id:int>, "text": "..." }
→ 200 { "status": "sent", "msg_id": <int>, "account_id": <int> }
→ 404 { "detail": "no delta account for bot '<bot_id>'" }
→ 502 { "detail": "send failed: ..." }         # backend/rpc error
```

Companion endpoints (same 404/502 semantics): `GET /healthz`, `POST /drain`,
`GET /contacts`, `GET /channels`, `POST /send_channel`, `POST /channel`,
`POST /channel/member`, `POST /react`.

## The reconciler + create-on-login model

DCF does **not** run a provisioning API. Following the chatmail model, a Delta account
*exists* as soon as an IMAP LOGIN with its credentials succeeds (Dovecot passdb
create-on-login). Each reconcile pass:

1. For every roster bot, mint-or-load a per-bot password (local secrets store) and
   perform an idempotent IMAP LOGIN against `DELTA_IMAP_HOST:DELTA_IMAP_PORT` as
   `localpart@DELTA_MAIL_DOMAIN` → the account is created-on-login if absent.
2. Diff desired-vs-existing localparts and **report** the prune set (accounts no longer in
   the roster). DCF never deletes server-side accounts; that's the mail server's lane.

Passwords are minted with a CSPRNG (never below the server's minimum length) and persisted
to a local mode-`600` secrets file (`DELTA_SECRETS_PATH`) so login is stable across
restarts. Point that path at a locket/secret mount in a real deploy.

### Per-realm channels

After onboarding accounts, each reconcile pass also ensures **one group chat per realm**
(that has a `realm_leads` entry): the realm's lead account creates a group named after the
realm and syncs in every roster member of that realm. Idempotent — matches an existing
channel by name and only adds missing members; a realm whose lead isn't onboarded yet is
retried next pass. The lead is the channel's "main" (routing wakes it on an unaddressed
message). A realm with no lead is skipped (nothing to route unaddressed messages to).

## Backup

A nightly loop calls the deltachat **imex** backup export
(`rpc.export_backup(accid, folder, None)` — a portable backup tar, *not* a raw SQLCipher
copy) for each live account into `DELTA_BACKUP_DIR`, naming each `<localpart>-<UTC>.tar`,
then prunes to the newest `DELTA_BACKUP_RETAIN` per account. The scheduling and rotation
logic is pure + unit-tested; only the one imex RPC call touches the live core.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `DELTA_MAIL_DOMAIN` | *(required)* | Mail domain accounts live under (e.g. `deltachat.example.net`) |
| `DELTA_IMAP_HOST` | `=DELTA_MAIL_DOMAIN` | IMAP host for create-on-login |
| `DELTA_IMAP_PORT` | `993` | IMAP port (implicit TLS) |
| `DELTA_SUBMISSION_HOST` | `=DELTA_IMAP_HOST` | SMTP submission host |
| `DELTA_SUBMISSION_PORT` | `587` | SMTP submission port |
| `DELTA_ROSTER_PATH` | `/config/roster.yaml` | Mounted roster YAML (bots + realm_leads) |
| `A2A_DIRECTORY_URL` | *(empty)* | a2a directory used to resolve a bot's live wake URL |
| `DATA_DIR` | `/data` | LOCAL account-DB + hold-queue dir (**never NFS** — SQLCipher) |
| `ACCOUNTS_DIR` | `$DATA_DIR/accounts` | deltachat accounts dir |
| `DELTA_SECRETS_PATH` | `$DATA_DIR/secrets.json` | Local per-bot password store (mode 600) |
| `DELTA_BACKUP_DIR` | `/backup` | imex backup output dir |
| `DELTA_BACKUP_RETAIN` | `7` | Backups kept per account |
| `DELTA_BACKUP_INTERVAL` | `86400` | Backup loop period (s) |
| `DELTA_RECONCILE_INTERVAL` | `3600` | Reconciler loop period (s) |
| `DELTA_RECONCILE_ON_START` | `1` | Reconcile once at boot (`1`/`0`) |
| `RELAY_HOST` / `RELAY_PORT` | `0.0.0.0` / `8080` | uvicorn bind (also honors `PORT`) |
| `DELTA_PASSWORD_MIN_LENGTH` | `9` | Server's minimum password length |
| `DELTA_USERNAME_MIN_LENGTH` / `_MAX_LENGTH` | `1` / `64` | Localpart length bounds |
| `RELAY_URL` | `http://localhost:8080` | Base URL the MCP tools call |
| `DELTA_MCP_HOST` / `DELTA_MCP_PORT` | `0.0.0.0` / `8000` | MCP `/mcp` (streamable-HTTP) bind |
| `DELTA_MCP_ALLOWED_HOSTS` | *(empty)* | Comma-separated Host allowlist for the `/mcp` server. **Empty (default) = DNS-rebinding protection OFF** — accepts any Host, correct for an internal-net deploy behind a gateway (the SDK default allows only `localhost`, which 421-rejects an in-cluster client connecting by service name). Set this (supports `host:*` port-wildcard) to re-enable + scope protection for a browser-exposed deploy. |
| `DELTA_MCP_ALLOWED_ORIGINS` | *(=hosts)* | Comma-separated Origin allowlist (only when `DELTA_MCP_ALLOWED_HOSTS` is set). |

### Roster file

The roster is **mounted, never baked**. See `roster.example.yaml`:

```yaml
bots:
  - id: alpha
    realm: example
  - id: beta
    realm: example
    localpart: beta-bot     # optional override; defaults to id
  - gamma                   # bare string → id=gamma, realm=default
realm_leads:
  example: alpha            # unaddressed "example"-realm messages wake only alpha
```

## Run

### Docker Compose (standalone example)

```bash
cp roster.example.yaml config/roster.yaml   # inject your roster (gitignored)
# edit docker-compose.yml env for your domain / imap host / directory
docker compose up --build
```

`/data` and `/backup` are **local** volumes (SQLCipher over NFS corrupts — never NFS).

### Direct

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
DELTA_MAIL_DOMAIN=deltachat.example.net \
DELTA_ROSTER_PATH=./config/roster.yaml \
DATA_DIR=./data DELTA_BACKUP_DIR=./backup \
  .venv/bin/python -m app.main
```

### Tests

```bash
.venv/bin/python -m pytest -q
```

The suite runs fully offline — every external dependency (rpc-server, IMAP, a2a HTTP) is
behind an injectable seam and faked in tests.

## Notes / caveats

Several deltachat JSON-RPC method names used by the default backend (contact/channel
enumeration, reactions) could not be signature-verified against reachable autodocs and are
isolated behind the `DeltaBackend` / `BackupBackend` seams with `# pragma: no cover` +
defensive `getattr` so an API drift is a one-place fix. **Verify against the deployed core
before production.** The imex backup call
(`export_backup(accid, folder, passphrase)`) **is** verified
([deltachat-bot/deltabot-cli-py autodocs](https://github.com/deltachat-bot/deltabot-cli-py)).
