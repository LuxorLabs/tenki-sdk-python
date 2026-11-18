from datetime import timedelta

import pytest

from tenki_sandbox.client import build_create_session_request


@pytest.mark.parametrize("value", [0, -5, timedelta(0)])
def test_non_positive_pause_retention_falls_back_to_the_workspace_limit(value):
    req = build_create_session_request(workspace_id="ws-1", pause_retention=value)
    assert not req.HasField("pause_retention")


def test_pause_retention_is_sent():
    req = build_create_session_request(
        workspace_id="ws-1", pause_retention=timedelta(hours=2)
    )
    assert req.pause_retention.seconds == 7200
