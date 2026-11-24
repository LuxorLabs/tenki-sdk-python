from __future__ import annotations

import pytest

from tenki_sandbox import SandboxWarningCode
from tenki_sandbox._convert import sandbox_info
from tenki_sandbox._pb import pb
from tenki_sandbox.warning import sandbox_warnings_from_proto

FUTURE_ENUM_VALUE = 99


@pytest.mark.parametrize(
    ("session", "mode", "reason"),
    [
        (pb.SandboxSession(), "UNSPECIFIED", "UNSPECIFIED"),
        (pb.SandboxSession(network_mode=pb.SESSION_NETWORK_MODE_FULL), "FULL", "UNSPECIFIED"),
        (
            pb.SandboxSession(
                network_mode=pb.SESSION_NETWORK_MODE_RESTRICTED,
                network_restriction_reason=pb.SESSION_NETWORK_RESTRICTION_REASON_NEVER_PURCHASED,
            ),
            "RESTRICTED",
            "NEVER_PURCHASED",
        ),
        (
            pb.SandboxSession(
                network_mode=FUTURE_ENUM_VALUE, network_restriction_reason=FUTURE_ENUM_VALUE
            ),
            "UNSPECIFIED",
            "UNSPECIFIED",
        ),
    ],
)
def test_sandbox_info_reads_the_network_mode(session, mode, reason):
    info = sandbox_info(session)

    assert info.network_mode == mode
    assert info.network_restriction_reason == reason


def test_restricted_network_warning_code():
    (warning,) = sandbox_warnings_from_proto(
        [
            pb.SandboxWarning(
                code=pb.SANDBOX_WARNING_CODE_RESTRICTED_NETWORK, message="restricted"
            )
        ]
    )

    assert warning.code == SandboxWarningCode.RESTRICTED_NETWORK
