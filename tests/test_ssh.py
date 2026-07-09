from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

import tenki_sandbox
from tenki_sandbox import Client, SSHCert, SSHConn, SSHUnavailableError
from tenki_sandbox._pb import pb, ssh_pb
from tenki_sandbox.errors import (
    InvalidStateError,
    PermissionDeniedError,
    SessionNotFoundError,
    UnauthorizedError,
)
from tenki_sandbox.ssh import (
    _build_gateway_ssh_url,
    _discover_ssh_gateway,
    _override_gateway_host,
    _raise_ws_connect_error,
)


# ---------------------------------------------------------------------------
# _build_gateway_ssh_url
# ---------------------------------------------------------------------------

class TestBuildGatewaySshUrl:
    def test_http_becomes_ws(self):
        url = _build_gateway_ssh_url("http://gateway.example", "sid-1")
        assert url == "ws://gateway.example/v1/ssh/sid-1"

    def test_https_becomes_wss(self):
        url = _build_gateway_ssh_url("https://gateway.example", "sid-1")
        assert url == "wss://gateway.example/v1/ssh/sid-1"

    def test_ws_stays_ws(self):
        url = _build_gateway_ssh_url("ws://gateway.example", "sid-1")
        assert url == "ws://gateway.example/v1/ssh/sid-1"

    def test_wss_stays_wss(self):
        url = _build_gateway_ssh_url("wss://gateway.example:8443", "sid-1")
        assert url == "wss://gateway.example:8443/v1/ssh/sid-1"

    def test_empty_input_returns_empty(self):
        assert _build_gateway_ssh_url("", "sid-1") == ""

    def test_no_scheme_defaults_to_wss(self):
        url = _build_gateway_ssh_url("gateway.example", "sid-1")
        assert url == "wss://gateway.example/v1/ssh/sid-1"

    def test_trailing_slash_stripped(self):
        url = _build_gateway_ssh_url("wss://gateway.example/", "sid-1")
        assert url == "wss://gateway.example/v1/ssh/sid-1"


# ---------------------------------------------------------------------------
# _override_gateway_host
# ---------------------------------------------------------------------------

class TestOverrideGatewayHost:
    def test_replaces_scheme_and_host(self):
        base = "wss://old-gw.example/v1/ssh/sid-1"
        result = _override_gateway_host(base, "wss://new-gw.example", "sid-1")
        assert result == "wss://new-gw.example/v1/ssh/sid-1"

    def test_preserves_path_from_base(self):
        base = "wss://old-gw.example/v1/ssh/session-42"
        result = _override_gateway_host(base, "wss://new-gw.example:9090", "session-42")
        assert result == "wss://new-gw.example:9090/v1/ssh/session-42"

    def test_empty_base_builds_fresh_url(self):
        result = _override_gateway_host("", "wss://new-gw.example", "sid-1")
        assert result == "wss://new-gw.example/v1/ssh/sid-1"

    def test_http_override_becomes_ws(self):
        base = "wss://old-gw.example/v1/ssh/sid-1"
        result = _override_gateway_host(base, "http://new-gw.example", "sid-1")
        assert result == "ws://new-gw.example/v1/ssh/sid-1"

    def test_no_scheme_override_defaults_to_wss(self):
        base = "wss://old-gw.example/v1/ssh/sid-1"
        result = _override_gateway_host(base, "new-gw.example", "sid-1")
        assert result == "wss://new-gw.example/v1/ssh/sid-1"


# ---------------------------------------------------------------------------
# _discover_ssh_gateway
# ---------------------------------------------------------------------------

class FakeGateway:
    def __init__(self, *, gateway_id="gw-1", region="us-east", healthy=True, ws_bridge_endpoint=""):
        self.gateway_id = gateway_id
        self.region = region
        self.healthy = healthy
        self.ws_bridge_endpoint = ws_bridge_endpoint


class FakeListResp:
    def __init__(self, gateways):
        self.gateways = gateways


class FakeSSHRPC:
    def __init__(self, resp=None, raises=None):
        self._resp = resp
        self._raises = raises
        self.calls = []

    def list_active_ssh_gateways(self, req):
        self.calls.append(req)
        if self._raises:
            raise self._raises
        return self._resp


class TestDiscoverSshGateway:
    def test_healthy_gateway_with_bridge_returns_url(self):
        gw = FakeGateway(healthy=True, ws_bridge_endpoint="wss://bridge.example")
        rpc = FakeSSHRPC(resp=FakeListResp([gw]))
        url = _discover_ssh_gateway(rpc, "sid-1")
        assert url == "wss://bridge.example/v1/ssh/sid-1"

    def test_unhealthy_gateway_skipped(self):
        gw = FakeGateway(healthy=False, ws_bridge_endpoint="wss://bridge.example")
        rpc = FakeSSHRPC(resp=FakeListResp([gw]))
        assert _discover_ssh_gateway(rpc, "sid-1") == ""

    def test_empty_bridge_skipped(self):
        gw = FakeGateway(healthy=True, ws_bridge_endpoint="")
        rpc = FakeSSHRPC(resp=FakeListResp([gw]))
        assert _discover_ssh_gateway(rpc, "sid-1") == ""

    def test_rpc_error_returns_empty(self):
        rpc = FakeSSHRPC(raises=RuntimeError("network error"))
        assert _discover_ssh_gateway(rpc, "sid-1") == ""

    def test_first_healthy_wins(self):
        gw1 = FakeGateway(healthy=False, ws_bridge_endpoint="wss://bad.example")
        gw2 = FakeGateway(healthy=True, ws_bridge_endpoint="wss://good.example")
        rpc = FakeSSHRPC(resp=FakeListResp([gw1, gw2]))
        url = _discover_ssh_gateway(rpc, "sid-1")
        assert url == "wss://good.example/v1/ssh/sid-1"

    def test_http_bridge_becomes_ws(self):
        gw = FakeGateway(healthy=True, ws_bridge_endpoint="http://bridge.example")
        rpc = FakeSSHRPC(resp=FakeListResp([gw]))
        url = _discover_ssh_gateway(rpc, "sid-1")
        assert url == "ws://bridge.example/v1/ssh/sid-1"


# ---------------------------------------------------------------------------
# SSHConn
# ---------------------------------------------------------------------------

class FakeWS:
    def __init__(self, frames=None):
        self._frames = list(frames or [])
        self.sent = []
        self.closed = False

    def recv_data(self):
        if not self._frames:
            raise EOFError("no more data")
        return self._frames.pop(0)

    def send_binary(self, data):
        self.sent.append(data)

    def close(self):
        self.closed = True


class TestSSHConn:
    def test_readable_and_writable(self):
        conn = SSHConn(FakeWS())
        assert conn.readable()
        assert conn.writable()

    def test_readinto_binary_frame(self):
        ws = FakeWS(frames=[(2, b"hello")])
        conn = SSHConn(ws)
        buf = bytearray(10)
        n = conn.readinto(buf)
        assert n == 5
        assert buf[:5] == b"hello"

    def test_readinto_close_frame_returns_zero(self):
        ws = FakeWS(frames=[(8, b"")])
        conn = SSHConn(ws)
        buf = bytearray(10)
        assert conn.readinto(buf) == 0

    def test_readinto_exception_returns_zero(self):
        ws = FakeWS(frames=[])  # will raise on recv_data
        conn = SSHConn(ws)
        buf = bytearray(10)
        assert conn.readinto(buf) == 0

    def test_readinto_leftover_bytes_remain(self):
        ws = FakeWS(frames=[(2, b"hello world")])
        conn = SSHConn(ws)
        buf = bytearray(5)
        n = conn.readinto(buf)
        assert n == 5
        assert buf[:5] == b"hello"
        # second read returns remainder without new recv_data
        buf2 = bytearray(10)
        n2 = conn.readinto(buf2)
        assert n2 == 6
        assert buf2[:6] == b" world"

    def test_write_delegates_to_send_binary(self):
        ws = FakeWS()
        conn = SSHConn(ws)
        n = conn.write(b"data")
        assert n == 4
        assert ws.sent == [b"data"]

    def test_write_empty_returns_zero(self):
        ws = FakeWS()
        conn = SSHConn(ws)
        assert conn.write(b"") == 0
        assert ws.sent == []

    def test_close_calls_ws_close(self):
        ws = FakeWS()
        conn = SSHConn(ws)
        conn.close()
        assert ws.closed
        assert conn.closed

    def test_close_is_idempotent(self):
        ws = FakeWS()
        conn = SSHConn(ws)
        conn.close()
        conn.close()  # should not raise

    def test_close_write_is_noop(self):
        conn = SSHConn(FakeWS())
        conn.close_write()  # no exception, no effect

    # paramiko socket-compat methods

    def test_settimeout_is_noop(self):
        conn = SSHConn(FakeWS())
        conn.settimeout(30)     # should not raise
        conn.settimeout(None)   # should not raise

    def test_recv_returns_bytes(self):
        ws = FakeWS(frames=[(2, b"hello")])
        conn = SSHConn(ws)
        data = conn.recv(10)
        assert data == b"hello"

    def test_recv_partial_reads(self):
        ws = FakeWS(frames=[(2, b"hello world")])
        conn = SSHConn(ws)
        first = conn.recv(5)
        assert first == b"hello"
        second = conn.recv(10)
        assert second == b" world"

    def test_send_delegates_to_write(self):
        ws = FakeWS()
        conn = SSHConn(ws)
        n = conn.send(b"data")
        assert n == 4
        assert ws.sent == [b"data"]

    def test_closed_property_mirrors_io_closed(self):
        conn = SSHConn(FakeWS())
        assert conn._closed is False
        conn.close()
        assert conn._closed is True


# ---------------------------------------------------------------------------
# _raise_ws_connect_error
# ---------------------------------------------------------------------------

class TestRaiseWsConnectError:
    def test_401_raises_unauthorized(self):
        with pytest.raises(UnauthorizedError):
            _raise_ws_connect_error(401, "unauthorized")

    def test_403_raises_permission_denied(self):
        with pytest.raises(PermissionDeniedError):
            _raise_ws_connect_error(403, "forbidden")

    def test_404_raises_session_not_found(self):
        with pytest.raises(SessionNotFoundError):
            _raise_ws_connect_error(404, "not found")

    def test_409_raises_invalid_state(self):
        with pytest.raises(InvalidStateError):
            _raise_ws_connect_error(409, "conflict")

    def test_other_raises_ssh_unavailable(self):
        with pytest.raises(SSHUnavailableError):
            _raise_ws_connect_error(500, "server error")

    def test_none_status_raises_ssh_unavailable(self):
        with pytest.raises(SSHUnavailableError):
            _raise_ws_connect_error(None, "generic failure")


# ---------------------------------------------------------------------------
# Client.ssh / Client.issue_ssh_cert
# ---------------------------------------------------------------------------

class FakeRPC:
    def __init__(self):
        self.requests = {
            "create_session": [],
            "get_session": [],
            "terminate_session": [],
        }
        self.issue_ssh_cert_resp = None
        self.list_gateways_resp = None

    def create_session(self, req):
        self.requests["create_session"].append(req)
        return pb.CreateSessionResponse(session=_session())

    def get_session(self, req):
        self.requests["get_session"].append(req)
        return pb.GetSessionResponse(session=_session())

    def terminate_session(self, req):
        self.requests["terminate_session"].append(req)
        return pb.TerminateSessionResponse(session=_session(pb.SESSION_STATE_TERMINATED))

    def issue_ssh_cert(self, req):
        return self.issue_ssh_cert_resp

    def list_active_ssh_gateways(self, req):
        if self.list_gateways_resp is None:
            return FakeListResp([])
        return self.list_gateways_resp


def _session(state=pb.SESSION_STATE_RUNNING):
    return pb.SandboxSession(
        id="session-1",
        state=state,
        owner_type="SERVICE",
        owner_id="self",
        name="test",
        project_id="proj-1",
        workspace_id="ws-1",
    )


class TestClientSsh:
    def _client(self, gateway_explicit=False, gateway_url="wss://gw.example"):
        rpc = FakeRPC()
        client = Client(rpc=rpc, auth_token="tk_test")
        client.gateway_url = gateway_url
        client._gateway_explicit = gateway_explicit
        return client, rpc

    def test_ssh_with_explicit_override_url(self):
        client, rpc = self._client(gateway_explicit=False)
        fake_ws = FakeWS()

        with patch("tenki_sandbox.ssh.open_ssh_conn") as mock_open:
            mock_open.return_value = SSHConn(fake_ws)
            conn = client.ssh("sid-1", gateway_url="wss://override.example")
            mock_open.assert_called_once()
            kwargs = mock_open.call_args
            assert kwargs[1]["override_gateway_url"] == "wss://override.example"
            assert kwargs[0][1] == "sid-1"

    def test_ssh_without_override_uses_gateway_url(self):
        client, rpc = self._client(gateway_explicit=False)
        fake_ws = FakeWS()

        with patch("tenki_sandbox.ssh.open_ssh_conn") as mock_open:
            mock_open.return_value = SSHConn(fake_ws)
            client.ssh("sid-1")
            kwargs = mock_open.call_args
            assert kwargs[0][2] == "wss://gw.example"
            assert kwargs[1]["override_gateway_url"] is None

    def test_ssh_skips_discovery_when_gateway_explicit(self):
        client, rpc = self._client(gateway_explicit=True)
        fake_ws = FakeWS()

        with patch("tenki_sandbox.ssh.open_ssh_conn") as mock_open:
            mock_open.return_value = SSHConn(fake_ws)
            client.ssh("sid-1")
            kwargs = mock_open.call_args
            assert kwargs[1]["gateway_address_explicit"] is True


class TestOpenSshConnSuppressOrigin:
    """open_ssh_conn must pass suppress_origin=True to ws.connect() to avoid
    the gateway's checkWebSocketOrigin rejecting non-preview-domain origins."""

    def test_suppress_origin_passed_to_ws_connect(self):
        from tenki_sandbox.ssh import open_ssh_conn

        fake_ws = MagicMock()
        fake_ws_lib = MagicMock()
        fake_ws_lib.WebSocket.return_value = fake_ws

        fake_rpc = MagicMock()
        fake_rpc.list_active_ssh_gateways.return_value = FakeListResp([])

        # open_ssh_conn does a local `import websocket` at call time, so
        # injecting into sys.modules is sufficient — no module reload needed.
        with patch.dict("sys.modules", {"websocket": fake_ws_lib}):
            open_ssh_conn(
                fake_rpc,
                "sid-1",
                "ws://gateway.example",
                "tk_test",
                gateway_address_explicit=True,
            )

        call_kwargs = fake_ws.connect.call_args[1]
        assert call_kwargs.get("suppress_origin") is True


class TestClientIssueSshCert:
    def _client(self):
        rpc = FakeRPC()
        client = Client(rpc=rpc, auth_token="tk_test")
        return client, rpc

    def test_maps_response_to_sshcert(self):
        client, rpc = self._client()
        rpc.issue_ssh_cert_resp = ssh_pb.IssueSandboxSSHCertResponse(
            ssh_cert="cert-data",
            ca_pub="ca-pub-data",
            cert_serial="serial-1",
        )
        cert = client.issue_ssh_cert("sid-1", "ssh-ed25519 AAAA")
        assert isinstance(cert, SSHCert)
        assert cert.ssh_cert == "cert-data"
        assert cert.ca_pub == "ca-pub-data"
        assert cert.cert_serial == "serial-1"
        assert cert.expires_at is None

    def test_strips_session_id_and_public_key(self):
        client, rpc = self._client()
        captured = []

        original = rpc.issue_ssh_cert
        def capture(req):
            captured.append(req)
            return ssh_pb.IssueSandboxSSHCertResponse(ssh_cert="c", ca_pub="p")
        rpc.issue_ssh_cert = capture

        client.issue_ssh_cert("  sid-1  ", "  ssh-ed25519 AAAA  ")
        assert captured[0].session_id == "sid-1"
        assert captured[0].public_key == "ssh-ed25519 AAAA"

    def test_ttl_sets_requested_ttl(self):
        client, rpc = self._client()
        captured = []

        def capture(req):
            captured.append(req)
            return ssh_pb.IssueSandboxSSHCertResponse(ssh_cert="c", ca_pub="p")
        rpc.issue_ssh_cert = capture

        client.issue_ssh_cert("sid-1", "ssh-ed25519 AAAA", ttl=3600.0)
        req = captured[0]
        assert req.HasField("requested_ttl")
        assert req.requested_ttl.seconds == 3600

    def test_no_ttl_leaves_requested_ttl_unset(self):
        client, rpc = self._client()
        captured = []

        def capture(req):
            captured.append(req)
            return ssh_pb.IssueSandboxSSHCertResponse(ssh_cert="c", ca_pub="p")
        rpc.issue_ssh_cert = capture

        client.issue_ssh_cert("sid-1", "ssh-ed25519 AAAA")
        req = captured[0]
        assert not req.HasField("requested_ttl")


# ---------------------------------------------------------------------------
# Sandbox.ssh / Sandbox.issue_ssh_cert
# ---------------------------------------------------------------------------

class TestSandboxSshDelegation:
    def _sandbox(self):
        rpc = FakeRPC()
        client = Client(rpc=rpc, auth_token="tk_test")
        client.gateway_url = "wss://gw.example"
        client._gateway_explicit = False
        sandbox = client.create(wait=False)
        return sandbox, client, rpc

    def test_sandbox_ssh_delegates_to_client(self):
        sandbox, client, rpc = self._sandbox()
        with patch.object(client, "ssh") as mock_ssh:
            mock_ssh.return_value = SSHConn(FakeWS())
            conn = sandbox.ssh()
            mock_ssh.assert_called_once_with("session-1", gateway_url=None)

    def test_sandbox_ssh_passes_gateway_url(self):
        sandbox, client, rpc = self._sandbox()
        with patch.object(client, "ssh") as mock_ssh:
            mock_ssh.return_value = SSHConn(FakeWS())
            sandbox.ssh(gateway_url="wss://override.example")
            mock_ssh.assert_called_once_with("session-1", gateway_url="wss://override.example")

    def test_sandbox_issue_ssh_cert_delegates_to_client(self):
        sandbox, client, rpc = self._sandbox()
        expected = SSHCert(ssh_cert="c", ca_pub="p")
        with patch.object(client, "issue_ssh_cert", return_value=expected) as mock_issue:
            cert = sandbox.issue_ssh_cert("ssh-ed25519 AAAA")
            mock_issue.assert_called_once_with("session-1", "ssh-ed25519 AAAA", ttl=None)
            assert cert is expected

    def test_sandbox_issue_ssh_cert_passes_ttl(self):
        sandbox, client, rpc = self._sandbox()
        expected = SSHCert(ssh_cert="c", ca_pub="p")
        with patch.object(client, "issue_ssh_cert", return_value=expected) as mock_issue:
            sandbox.issue_ssh_cert("ssh-ed25519 AAAA", ttl=600.0)
            mock_issue.assert_called_once_with("session-1", "ssh-ed25519 AAAA", ttl=600.0)


# ---------------------------------------------------------------------------
# Public API exports
# ---------------------------------------------------------------------------

class TestPublicExports:
    def test_sshconn_exported(self):
        assert tenki_sandbox.SSHConn is SSHConn

    def test_sshcert_exported(self):
        assert tenki_sandbox.SSHCert is SSHCert

    def test_sshunavailableerror_exported(self):
        assert tenki_sandbox.SSHUnavailableError is SSHUnavailableError
