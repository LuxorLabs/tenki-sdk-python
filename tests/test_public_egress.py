import pytest

from tenki_sandbox._convert import public_egress_acknowledged, session_egress_policy
from tenki_sandbox.egress_policies import _body


def test_public_egress_preserves_presence():
    assert not _body(None, None, False, None, None).HasField("public_egress")
    denied = _body(None, None, False, None, None, {})
    assert denied.HasField("public_egress")
    assert session_egress_policy(denied).public_egress == {"tcp_ports": (), "udp_ports": ()}


def test_session_egress_keeps_immutable_ports():
    from tenki_sandbox.models import SessionEgressPolicy

    original = {"tcp_ports": [443]}
    policy = SessionEgressPolicy(public_egress=original)
    original["tcp_ports"].append(80)
    assert policy.public_egress["tcp_ports"] == (443,)
    assert isinstance(hash(policy), int)


def test_public_egress_acknowledgement_rejects_old_or_wider_server():
    requested = {"tcp_ports": [80, 443]}
    assert not public_egress_acknowledged(requested, None)
    assert not public_egress_acknowledged(requested, {"tcp_ports": [22]})
    assert public_egress_acknowledged(requested, {"tcp_ports": [443]})
    assert public_egress_acknowledged(None, None)


def test_public_egress_cleanup_passes_transport_deadline():
    from types import SimpleNamespace
    import pytest
    from tenki_sandbox._compatibility import cleanup_unsupported_public_egress
    calls = []
    rpc = SimpleNamespace(_accepts_call_timeout=True, terminate_session=lambda req, **kwargs: calls.append((req.session_id, kwargs)))
    with pytest.raises(RuntimeError, match="does not support"):
        cleanup_unsupported_public_egress(rpc, "session")
    assert calls == [("session", {"timeout": 10.0})]


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_public_egress_cleanup_survives_cancellation(fail):
    import asyncio
    from types import SimpleNamespace
    from tenki_sandbox._compatibility import cleanup_unsupported_public_egress_async
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def terminate(req, *, timeout):
        assert timeout == 10.0
        entered.set()
        await release.wait()
        finished.set()
        if fail:
            raise RuntimeError("cleanup rejected")
    task = asyncio.create_task(cleanup_unsupported_public_egress_async(SimpleNamespace(_accepts_call_timeout=True, terminate_session=terminate), "session"))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()


def test_public_egress_policy_compatibility_acknowledgement():
    from types import SimpleNamespace
    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2 as pb
    from tenki_sandbox.egress_policies import EgressPolicies
    saved = pb.EgressPolicy(name="ports", egress=_body(None, None, False, None, None, {"tcp_ports": [443]}))
    rpc = SimpleNamespace(
        create_egress_policy_with_public_egress=lambda req: pb.CreateEgressPolicyWithPublicEgressResponse(response=pb.CreateEgressPolicyResponse(policy=saved)),
        update_egress_policy_with_public_egress=lambda req: pb.UpdateEgressPolicyWithPublicEgressResponse(response=pb.UpdateEgressPolicyResponse(policy=saved)),
    )
    policies = EgressPolicies(rpc)
    assert policies.create("ports", public_egress={"tcp_ports": [443]}).egress.public_egress["tcp_ports"] == (443,)
    assert policies.update("ports", expected_revision=1, public_egress={"tcp_ports": [443]}).egress.public_egress["tcp_ports"] == (443,)


@pytest.mark.parametrize(
    "options, public_present",
    [
        ({"public_egress": {}}, True),
        ({"public_egress": {"tcp_ports": [443]}}, True),
        ({"allow_domains": ["example.com"]}, False),
        ({"allow_cidrs": ["192.0.2.0/24"]}, False),
        ({"intercept_https": True}, False),
        ({"allowed_ports": [8443]}, False),
    ],
)
def test_inline_create_policy_preserves_wire_presence(options, public_present):
    from tenki_sandbox.client import build_create_session_request

    request = build_create_session_request(**options)
    assert request.HasField("egress")
    assert request.egress.HasField("public_egress") is public_present


def test_inline_create_policy_omits_empty_legacy_controls():
    from tenki_sandbox.client import build_create_session_request

    request = build_create_session_request(allow_domains=[], allow_cidrs=[], allowed_ports=[], http_rules=[])
    assert not request.HasField("egress")
