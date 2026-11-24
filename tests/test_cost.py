from __future__ import annotations

from datetime import datetime, timezone

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

import tenki_sandbox
from tenki_sandbox import Client, SessionCost, SessionCostLine, SessionCostBatch
from tenki_sandbox._convert import session_cost
from tenki_sandbox._pb import pb
from tenki_sandbox._transport import GrpcSandboxRPC


def _at(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _final_cost(session_id: str = "session-1") -> pb.SessionCost:
    return pb.SessionCost(
        session_id=session_id,
        status=pb.SESSION_COST_STATUS_FINAL,
        total_usd="0.01234567",
        lines=[
            pb.SessionCostLine(
                item=pb.SESSION_COST_ITEM_CPU,
                quantity="7200",
                unit=pb.SESSION_COST_UNIT_VCPU_SECOND,
                amount_usd="0.01",
                started_at=Timestamp(seconds=1_700_000_000),
                ended_at=Timestamp(seconds=1_700_003_600),
            ),
            pb.SessionCostLine(
                item=pb.SESSION_COST_ITEM_MEMORY,
                quantity="14400.5",
                unit=pb.SESSION_COST_UNIT_GIB_SECOND,
                amount_usd="0.002",
                started_at=Timestamp(seconds=1_700_000_000),
                ended_at=Timestamp(seconds=1_700_003_600),
            ),
            pb.SessionCostLine(
                item=pb.SESSION_COST_ITEM_DISK,
                quantity="36000",
                unit=pb.SESSION_COST_UNIT_GIB_SECOND,
                amount_usd="0.0003",
                started_at=Timestamp(seconds=1_700_000_000),
                ended_at=Timestamp(seconds=1_700_003_600),
            ),
            pb.SessionCostLine(
                item=pb.SESSION_COST_ITEM_PAUSED_STORAGE,
                quantity="600",
                unit=pb.SESSION_COST_UNIT_GIB_SECOND,
                amount_usd="0.00004567",
                started_at=Timestamp(seconds=1_700_003_600),
                ended_at=Timestamp(seconds=1_700_004_200),
            ),
        ],
        as_of=Timestamp(seconds=1_700_004_200),
        billed_running_seconds=3600,
        billed_paused_seconds=600,
    )


class CostRPC:
    def __init__(self) -> None:
        self.request = None

    def get_session_cost(self, request):
        self.request = request
        return pb.GetSessionCostResponse(cost=_final_cost(request.session_id))

    def batch_get_session_costs(self, request):
        self.request = request
        return pb.BatchGetSessionCostsResponse(
            costs=[
                _final_cost("session-1"),
                pb.SessionCost(session_id="session-2", status=pb.SESSION_COST_STATUS_UNAVAILABLE),
            ],
            missing_session_ids=["session-3"],
        )

    def get_session(self, request):
        return pb.GetSessionResponse(session=pb.SandboxSession(id=request.session_id))


def test_get_session_cost_maps_every_field():
    rpc = CostRPC()
    cost = Client(rpc=rpc).get_session_cost("session-1")

    assert rpc.request.session_id == "session-1"
    assert cost == SessionCost(
        session_id="session-1",
        status="FINAL",
        total_usd="0.01234567",
        lines=(
            SessionCostLine("CPU", "7200", "VCPU_SECOND", "0.01", _at(1_700_000_000), _at(1_700_003_600)),
            SessionCostLine("MEMORY", "14400.5", "GIB_SECOND", "0.002", _at(1_700_000_000), _at(1_700_003_600)),
            SessionCostLine("DISK", "36000", "GIB_SECOND", "0.0003", _at(1_700_000_000), _at(1_700_003_600)),
            SessionCostLine(
                "PAUSED_STORAGE", "600", "GIB_SECOND", "0.00004567", _at(1_700_003_600), _at(1_700_004_200)
            ),
        ),
        as_of=_at(1_700_004_200),
        billed_running_seconds=3600,
        billed_paused_seconds=600,
    )


def test_session_cost_keeps_unset_optional_fields_none():
    cost = session_cost(
        pb.SessionCost(
            session_id="session-1",
            status=pb.SESSION_COST_STATUS_ACCRUING,
            lines=[
                pb.SessionCostLine(
                    item=pb.SESSION_COST_ITEM_CPU,
                    quantity="30",
                    unit=pb.SESSION_COST_UNIT_VCPU_SECOND,
                    amount_usd="0.0001",
                    started_at=Timestamp(seconds=1_700_000_000),
                )
            ],
        )
    )

    assert cost.status == "ACCRUING"
    assert cost.total_usd is None
    assert cost.as_of is None
    assert cost.billed_running_seconds == 0
    assert cost.billed_paused_seconds == 0
    assert cost.lines[0].started_at == _at(1_700_000_000)
    assert cost.lines[0].ended_at is None


def test_session_cost_distinguishes_zero_total_from_unset():
    cost = session_cost(pb.SessionCost(session_id="session-1", total_usd="0"))

    assert cost.total_usd == "0"
    assert cost.status == "UNSPECIFIED"
    assert cost.lines == ()


def test_session_cost_maps_unknown_enum_values_to_unspecified():
    proto = pb.SessionCost(session_id="session-1", lines=[pb.SessionCostLine()])
    proto.status = 99
    proto.lines[0].item = 99
    proto.lines[0].unit = 99
    cost = session_cost(proto)

    assert cost.status == "UNSPECIFIED"
    assert cost.lines[0].item == "UNSPECIFIED"
    assert cost.lines[0].unit == "UNSPECIFIED"
    assert cost.lines[0].started_at is None


def test_batch_get_session_costs_returns_costs_and_missing_ids():
    rpc = CostRPC()
    result = Client(rpc=rpc).batch_get_session_costs((" session-1", "session-2 ", "session-3"))

    assert list(rpc.request.session_ids) == ["session-1", "session-2", "session-3"]
    assert rpc.request.workspace_id == ""
    assert isinstance(result, SessionCostBatch)
    assert [cost.session_id for cost in result.costs] == ["session-1", "session-2"]
    assert result.costs[0].total_usd == "0.01234567"
    assert result.costs[1].status == "UNAVAILABLE"
    assert result.costs[1].total_usd is None
    assert result.costs[1].lines == ()
    assert result.missing_session_ids == ("session-3",)


def test_batch_get_session_costs_rejects_a_single_string():
    rpc = CostRPC()
    with pytest.raises(TypeError):
        Client(rpc=rpc).batch_get_session_costs("session-1")


def test_batch_get_session_costs_sends_workspace_id():
    rpc = CostRPC()
    Client(rpc=rpc).batch_get_session_costs(["session-1"], workspace_id=" ws-1 ")

    assert rpc.request.workspace_id == "ws-1"


def test_sandbox_cost_uses_its_session_id():
    rpc = CostRPC()
    sandbox = Client(rpc=rpc).get("session-1")
    cost = sandbox.cost()

    assert cost.session_id == "session-1"
    assert rpc.request.session_id == "session-1"


def test_session_cost_runtime_descriptors_and_transport_methods():
    assert {"session_id", "status", "total_usd", "lines", "as_of", "billed_running_seconds", "billed_paused_seconds"} == set(
        pb.SessionCost.DESCRIPTOR.fields_by_name
    )
    assert {"item", "quantity", "unit", "amount_usd", "started_at", "ended_at"} == set(
        pb.SessionCostLine.DESCRIPTOR.fields_by_name
    )
    assert GrpcSandboxRPC._methods["get_session_cost"] == "GetSessionCost"
    assert GrpcSandboxRPC._methods["batch_get_session_costs"] == "BatchGetSessionCosts"


def test_session_cost_types_are_exported():
    for name in (
        "SessionCost",
        "SessionCostItem",
        "SessionCostLine",
        "SessionCostStatus",
        "SessionCostUnit",
        "SessionCostBatch",
    ):
        assert name in tenki_sandbox.__all__
