import warnings

import pytest

from tenki_sandbox.client import build_create_session_request


def test_idle_timeout_minutes_warns_and_is_not_sent_on_the_wire():
    with pytest.warns(DeprecationWarning, match="idle_timeout_minutes is deprecated"):
        req = build_create_session_request(workspace_id="ws-1", idle_timeout_minutes=15)

    assert not req.HasField("idle_timeout_minutes")


def test_create_request_is_silent_when_idle_timeout_minutes_is_unset():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        req = build_create_session_request(workspace_id="ws-1")

    assert not req.HasField("idle_timeout_minutes")
