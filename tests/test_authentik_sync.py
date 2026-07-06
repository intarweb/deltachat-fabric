"""Unit tests for the roster→Authentik service-account sync + the op-connect access layer.

NO live network: the Authentik API + op-connect are mocked via httpx.MockTransport; the sync
orchestration is driven with fakes. Verifies the flow matches the verified Authentik API +
the auth owner's op-connect contract (fetch-by-title, PUT read-modify-write) and the idempotent
create-or-reuse + set_password + email/attrs + Bots-group logic.
"""
from __future__ import annotations

import json

import httpx

from app.authentik_sync import AuthentikClient, sync_bot, sync_roster
from app.config import BotSpec, Config
from app.opconnect import OpConnectStore

GROUP = "00000000-0000-0000-0000-000000000000"


# ---- fakes for the sync orchestration -------------------------------------------------------


class _FakeClient:
    def __init__(self, existing: dict | None = None):
        self._existing = existing or {}      # username -> pk
        self.created: list[str] = []
        self.passwords: dict[int, str] = {}
        self.updates: dict[int, dict] = {}   # pk -> PATCH body (email + attributes)
        self.group_adds: list[tuple] = []
        self._next = 100

    def find_user_pk(self, username):
        return self._existing.get(username)

    def create_service_account(self, name):
        self._next += 1
        self._existing[name] = self._next
        self.created.append(name)
        return self._next

    def set_password(self, pk, password):
        self.passwords[pk] = password

    def update_user(self, pk, body):
        self.updates[pk] = body

    def add_to_group(self, group_uuid, pk):
        self.group_adds.append((group_uuid, pk))


class _FakeStore:
    def get_or_create(self, bot):
        return f"pw-{bot}-123456789"


def test_sync_bot_creates_with_pw_email_attrs_and_group():
    client, store = _FakeClient(), _FakeStore()
    out = sync_bot(client, store, "alpha", "alpha@d.example", "pantheon", GROUP)
    assert out["created"] is True
    assert client.created == ["alpha"]
    pk = out["pk"]
    assert client.passwords[pk] == "pw-alpha-123456789"               # fixed pw from the store
    # 🔴 email + attrs set together (email required for the mail server's mail=<addr> lookup)
    assert client.updates[pk] == {"email": "alpha@d.example",
                                  "attributes": {"is_bot": True, "realm": "pantheon"}}
    assert client.group_adds == [(GROUP, pk)]                          # added to Bots


def test_sync_bot_reuses_existing_and_does_not_recreate():
    client = _FakeClient(existing={"beta": 7})
    out = sync_bot(client, _FakeStore(), "beta", "beta@d.example", "pantheon", GROUP)
    assert out == {"bot": "beta", "pk": 7, "created": False}
    assert client.created == []                                        # NOT recreated
    assert client.updates[7]["email"] == "beta@d.example"             # still (re)sets email+attrs
    assert client.group_adds == [(GROUP, 7)]


def test_sync_roster_dedupes_derives_email_and_survives_a_bad_bot():
    cfg = Config(mail_domain="deltachat.example.net", imap_host="m",
                 roster=[BotSpec(id="a", realm="pantheon"), BotSpec(id="b", realm="pantheon"),
                         BotSpec(id="a", realm="pantheon")])           # dupe a

    class Boom(_FakeClient):
        def create_service_account(self, name):
            if name == "b":
                raise RuntimeError("authentik 500")
            return super().create_service_account(name)

    client = Boom()
    res = sync_roster(cfg, _FakeStore(), client, GROUP)
    bots = {r["bot"]: r for r in res}
    assert set(bots) == {"a", "b"}                                     # deduped to 2
    assert bots["a"]["created"] is True
    assert bots["b"].get("error") is True                              # bad bot caught, pass survived
    # email derived from localpart@mail_domain
    assert client.updates[bots["a"]["pk"]]["email"] == "a@deltachat.example.net"


# ---- AuthentikClient against a mocked Authentik API -----------------------------------------


def test_authentik_client_hits_the_verified_endpoints():
    calls: list[tuple] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        p = request.url.path
        if p == "/api/v3/core/users/" and request.method == "GET":
            return httpx.Response(200, json={"results": []})            # not found → create
        if p == "/api/v3/core/users/service_account/":
            return httpx.Response(200, json={"user_pk": 42, "username": "alpha"})
        if p == "/api/v3/core/users/42/set_password/":
            return httpx.Response(204)
        if p == "/api/v3/core/users/42/" and request.method == "PATCH":
            return httpx.Response(200, json={"pk": 42})
        if p == f"/api/v3/core/groups/{GROUP}/add_user/":
            return httpx.Response(204)
        return httpx.Response(404)

    c = AuthentikClient("https://auth.test", "tok",
                        client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert c.find_user_pk("alpha") is None
    assert c.create_service_account("alpha") == 42
    c.set_password(42, "s3cret-pw-9")
    c.update_user(42, {"email": "alpha@d.example", "attributes": {"is_bot": True}})
    c.add_to_group(GROUP, 42)
    paths = set(calls)
    assert ("POST", "/api/v3/core/users/service_account/") in paths
    assert ("POST", "/api/v3/core/users/42/set_password/") in paths
    assert ("PATCH", "/api/v3/core/users/42/") in paths
    assert ("POST", f"/api/v3/core/groups/{GROUP}/add_user/") in paths


def test_authentik_client_find_returns_existing_pk():
    def handler(request):
        return httpx.Response(200, json={"results": [{"pk": 9, "username": "beta"}]})

    c = AuthentikClient("https://auth.test", "tok",
                        client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert c.find_user_pk("beta") == 9


# ---- op-connect: fetch-by-title + PUT read-modify-write -------------------------------------


def _op_handler(item_body: dict, *, writes: list):
    """Mock op-connect: list-by-title → [{id}], get item → item_body, PUT → record body."""
    def handler(request: httpx.Request) -> httpx.Response:
        p = request.url.path
        if request.method == "GET" and p.endswith("/items"):
            return httpx.Response(200, json=[{"id": "IID", "title": "creds"}])   # title filter
        if request.method == "GET" and p.endswith("/items/IID"):
            return httpx.Response(200, json=item_body)
        if request.method == "PUT" and p.endswith("/items/IID"):
            writes.append(json.loads(request.content.decode()))
            return httpx.Response(200, json={})
        return httpx.Response(404)
    return handler


def test_opconnect_store_returns_existing_field_no_write():
    writes: list = []
    body = {"id": "IID", "fields": [{"label": "alpha", "type": "CONCEALED", "value": "stored-pw-abc"}]}
    store = OpConnectStore(base_url="http://op.test", token="t", vault="v", item="creds",
                           client=httpx.Client(transport=httpx.MockTransport(_op_handler(body, writes=writes))))
    assert store.get_or_create("alpha") == "stored-pw-abc"
    assert writes == []                                                # existing → no PUT


def test_opconnect_store_mints_and_puts_when_absent():
    writes: list = []
    body = {"id": "IID", "fields": []}
    store = OpConnectStore(base_url="http://op.test", token="t", vault="v", item="creds",
                           client=httpx.Client(transport=httpx.MockTransport(_op_handler(body, writes=writes))),
                           password_min_length=9)
    pw = store.get_or_create("gamma")
    assert len(pw) >= 9                                                # minted a real pw
    assert len(writes) == 1                                            # PUT read-modify-write
    fields = {f["label"]: f for f in writes[0]["fields"]}
    assert fields["gamma"]["type"] == "CONCEALED" and fields["gamma"]["value"] == pw


def test_read_authentik_creds_from_op_item():
    from app.opconnect import read_authentik_creds
    body = {"id": "IID", "fields": [
        {"label": "authentik_url", "value": "https://authentik.example.com"},
        {"label": "credential", "type": "CONCEALED", "value": "tok-xyz"}]}
    url, tok = read_authentik_creds(
        "http://op.test", "t", "v", "token-item",
        client=httpx.Client(transport=httpx.MockTransport(_op_handler(body, writes=[]))))
    assert url == "https://authentik.example.com" and tok == "tok-xyz"
