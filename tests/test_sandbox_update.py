from __future__ import annotations

import warnings

import pytest

from tenki_sandbox import Client, SandboxWarningCode, SandboxWarningNotification
from tenki_sandbox._pb import pb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(**kwargs) -> pb.SandboxSession:
    defaults = dict(id="sess-1", name="my-box", state=pb.SESSION_STATE_RUNNING)
    defaults.update(kwargs)
    return pb.SandboxSession(**defaults)


class FakeRPC:
    def __init__(self):
        self.update_request = None
        self.ssh_keys_request = None
        self.update_warnings = []

    def create_session(self, req):
        return pb.CreateSessionResponse(session=_make_session())

    def get_session(self, req):
        return pb.GetSessionResponse(session=_make_session())

    def update_session(self, req):
        self.update_request = req
        response = pb.UpdateSessionResponse(
            session=_make_session(name=req.name or "my-box")
        )
        for code, message in self.update_warnings:
            response.warnings.add(code=code, message=message)
        return response

    def update_ssh_authorized_keys(self, req):
        self.ssh_keys_request = req
        return pb.UpdateSSHAuthorizedKeysResponse()


# ---------------------------------------------------------------------------
# sandbox.update
# ---------------------------------------------------------------------------

def test_update_name():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="new-name")
    assert rpc.update_request.session_id == "sess-1"
    assert rpc.update_request.name == "new-name"


def test_update_tags_set():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=["gpu", "python"])
    req = rpc.update_request
    assert list(req.tags) == ["gpu", "python"]
    assert not req.clear_tags


def test_update_tags_empty_clears():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=[])
    req = rpc.update_request
    assert req.clear_tags is True
    assert list(req.tags) == []


def test_update_tags_normalized():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(tags=["  GPU ", "Python"])
    assert list(rpc.update_request.tags) == ["gpu", "python"]


def test_update_tags_none_not_touched():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="x")
    req = rpc.update_request
    assert not req.clear_tags
    assert list(req.tags) == []


def test_update_sticky():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(sticky=True)
    assert rpc.update_request.sticky is True


def test_update_maps_max_duration_with_sticky_false():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")

    sandbox.update(sticky=False, max_duration=90.5)

    assert rpc.update_request.HasField("sticky")
    assert rpc.update_request.sticky is False
    assert rpc.update_request.max_duration.seconds == 90
    assert rpc.update_request.max_duration.nanos == 500_000_000


@pytest.mark.parametrize("max_duration", [0, -1])
def test_update_rejects_nonpositive_max_duration_before_rpc(max_duration):
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")

    with pytest.raises(ValueError, match="max_duration must be greater than zero"):
        sandbox.update(sticky=False, max_duration=max_duration)

    assert rpc.update_request is None


def test_update_requires_sticky_with_max_duration_before_rpc():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")

    with pytest.raises(
        ValueError, match="sticky must be set when max_duration is provided"
    ):
        sandbox.update(max_duration=90)

    assert rpc.update_request is None


def test_update_mutates_info():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update(name="updated")
    assert sandbox.name == "updated"


def test_update_retains_warnings_and_is_safe_with_warnings_as_errors():
    rpc = FakeRPC()
    rpc.update_warnings = [
        (pb.SANDBOX_WARNING_CODE_MAX_DURATION_CAPPED, "max duration capped")
    ]
    sandbox = Client(rpc=rpc).get("sess-1")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SandboxWarningNotification)
        sandbox.update(sticky=False)

    assert sandbox.warnings[-1].code == SandboxWarningCode.MAX_DURATION_CAPPED
    assert sandbox.warnings[-1].message == "max duration capped"


def test_update_ignores_custom_warning_handler_errors(caplog):
    rpc = FakeRPC()
    rpc.update_warnings = [
        (pb.SANDBOX_WARNING_CODE_MAX_DURATION_CAPPED, "max duration capped")
    ]

    def warning_handler(_warning):
        raise RuntimeError("handler failed")

    sandbox = Client(rpc=rpc, warning_handler=warning_handler).get("sess-1")
    with caplog.at_level("ERROR", logger="tenki_sandbox"):
        sandbox.update(sticky=False)

    assert sandbox.warnings[-1].code == SandboxWarningCode.MAX_DURATION_CAPPED
    assert "sandbox warning handler failed" in caplog.messages


def test_sandbox_warning_codes_include_update_codes():
    assert (
        SandboxWarningCode.STICKY_OVERRIDES_IDLE_TIMEOUT
        == "STICKY_OVERRIDES_IDLE_TIMEOUT"
    )
    assert SandboxWarningCode.MAX_DURATION_CAPPED == "MAX_DURATION_CAPPED"


def test_update_tags_alias():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update_tags("a", "b")
    assert list(rpc.update_request.tags) == ["a", "b"]


# ---------------------------------------------------------------------------
# sandbox.update_ssh_authorized_keys
# ---------------------------------------------------------------------------

def test_update_ssh_authorized_keys():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    keys = ["ssh-ed25519 AAAA...", "ssh-rsa BBBB..."]
    sandbox.update_ssh_authorized_keys(keys)
    req = rpc.ssh_keys_request
    assert req.session_id == "sess-1"
    assert list(req.ssh_authorized_keys) == keys


def test_update_ssh_authorized_keys_empty():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).get("sess-1")
    sandbox.update_ssh_authorized_keys([])
    assert list(rpc.ssh_keys_request.ssh_authorized_keys) == []
