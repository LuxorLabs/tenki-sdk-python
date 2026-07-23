"""SSHGatewayClientService goes over the Connect protocol (plain HTTPS POST),
matching the Go SDK; edge ingress does not route raw gRPC for this service."""
from __future__ import annotations

import http.server
import json
import socketserver
import threading

import pytest

from tenki_sandbox._internal.proto.tenki.sandbox.v1 import ssh_gateway_client_pb2 as ssh_pb
from tenki_sandbox._transport import GrpcConfig, GrpcSandboxRPC
from tenki_sandbox.errors import UnauthorizedError

SSH_SERVICE = "/tenki.sandbox.v1.SSHGatewayClientService"
SSH_LEGACY_SERVICE = "/tenki.sandbox.v1.SSHGatewayService"


class _Handler(http.server.BaseHTTPRequestHandler):
    seen: list[dict] = []
    # When True, only the legacy combined service path is mounted (pre-#3866 engine).
    legacy_only = False

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        type(self).seen.append({
            "path": self.path,
            "content_type": self.headers.get("Content-Type"),
            "authorization": self.headers.get("Authorization"),
            "connect_version": self.headers.get("Connect-Protocol-Version"),
            "body": body,
        })
        if type(self).legacy_only and self.path.startswith(SSH_SERVICE + "/"):
            payload = b"404 page not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path in (f"{SSH_SERVICE}/IssueSandboxSSHCert", f"{SSH_LEGACY_SERVICE}/IssueSandboxSSHCert"):
            resp = ssh_pb.IssueSandboxSSHCertResponse(ssh_cert="ssh-ed25519-cert c", ca_pub="ca-pub")
            payload = resp.SerializeToString()
            self.send_response(200)
            self.send_header("Content-Type", "application/proto")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        payload = json.dumps({"code": "unauthenticated", "message": "bad token"}).encode()
        self.send_response(401)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture()
def connect_server():
    _Handler.seen = []
    _Handler.legacy_only = False
    with socketserver.TCPServer(("127.0.0.1", 0), _Handler) as srv:
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{srv.server_address[1]}"
        finally:
            srv.shutdown()
            thread.join()


def test_issue_ssh_cert_uses_connect_protocol(connect_server):
    rpc = GrpcSandboxRPC(GrpcConfig(base_url=connect_server, auth_token="tok-123"))
    req = ssh_pb.IssueSandboxSSHCertRequest(session_id="sid-1", public_key="ssh-ed25519 AAAA")
    resp = rpc.issue_ssh_cert(req)
    assert resp.ssh_cert == "ssh-ed25519-cert c"
    assert resp.ca_pub == "ca-pub"

    seen = _Handler.seen[0]
    assert seen["path"] == f"{SSH_SERVICE}/IssueSandboxSSHCert"
    assert seen["content_type"] == "application/proto"
    assert seen["connect_version"] == "1"
    assert seen["authorization"] == "Bearer tok-123"
    parsed = ssh_pb.IssueSandboxSSHCertRequest()
    parsed.ParseFromString(seen["body"])
    assert parsed.session_id == "sid-1"


def test_falls_back_to_legacy_service_path(connect_server):
    _Handler.legacy_only = True
    rpc = GrpcSandboxRPC(GrpcConfig(base_url=connect_server, auth_token="tok-123"))
    resp = rpc.issue_ssh_cert(ssh_pb.IssueSandboxSSHCertRequest(session_id="sid-1", public_key="k"))
    assert resp.ssh_cert == "ssh-ed25519-cert c"
    paths = [s["path"] for s in _Handler.seen]
    assert paths == [
        f"{SSH_SERVICE}/IssueSandboxSSHCert",
        f"{SSH_LEGACY_SERVICE}/IssueSandboxSSHCert",
    ]


def test_connect_error_maps_to_sdk_error(connect_server):
    rpc = GrpcSandboxRPC(GrpcConfig(base_url=connect_server, auth_token="tok-123"))
    with pytest.raises(UnauthorizedError, match="bad token"):
        rpc.list_active_ssh_gateways(ssh_pb.ListActiveSSHGatewaysRequest())


# ------------------------------------------------- streaming deadline (sync)

from tenki_sandbox._pb import pb  # noqa: E402


class _TimeoutStub:
    def __init__(self):
        self.calls = []

    def CreateSession(self, request, metadata=None, timeout=None):
        self.calls.append(("CreateSession", timeout))
        return pb.CreateSessionResponse()

    def WaitSession(self, request, metadata=None, timeout=None):
        self.calls.append(("WaitSession", timeout))
        return iter(())


def test_sync_streaming_rpc_does_not_inherit_client_timeout():
    # The client-wide timeout is a unary deadline; long-lived streams must not
    # inherit it or a control-plane tunnel dies with DEADLINE_EXCEEDED.
    rpc = GrpcSandboxRPC(GrpcConfig(base_url="http://localhost:8090", auth_token="tk", timeout=30))
    rpc._stub = _TimeoutStub()

    rpc.create_session(pb.CreateSessionRequest())  # unary keeps the deadline
    assert rpc._stub.calls[-1] == ("CreateSession", 30)

    list(rpc.wait_session(pb.WaitSessionRequest()))  # stream: no default deadline
    assert rpc._stub.calls[-1] == ("WaitSession", None)

    list(rpc.wait_session(pb.WaitSessionRequest(), timeout=5))  # explicit honored
    assert rpc._stub.calls[-1] == ("WaitSession", 5)
