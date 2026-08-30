from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from tenki_sandbox import Client
from tenki_sandbox._pb import pb


class MetricsRPC:
    def __init__(self) -> None:
        self.request = None

    def get_session_metrics(self, request):
        self.request = request
        return pb.GetSessionMetricsResponse(
            session_id="session-1",
            requested_window=Duration(seconds=300, nanos=250_000_000),
            cpu=pb.SessionCPUUsageAverage(
                limit_cores=2,
                sample_count=3,
                observed_duration=Duration(seconds=60, nanos=500_000_000),
                coverage_percent=75.5,
                first_sample_at=Timestamp(seconds=1_700_000_000),
                last_sample_at=Timestamp(seconds=1_700_000_060),
            ),
            memory=pb.SessionMemoryUsageAverage(
                average_bytes=512,
                limit_bytes=1024,
                coverage_percent=80.25,
                first_sample_at=Timestamp(seconds=1_700_000_010),
                last_sample_at=Timestamp(seconds=1_700_000_070),
            ),
        )

    def get_session(self, request):
        return pb.GetSessionResponse(session=pb.SandboxSession(id=request.session_id))


def test_get_session_metrics_maps_optional_values_and_window():
    rpc = MetricsRPC()
    metrics = Client(rpc=rpc).get_session_metrics("session-1", window=300.25)

    assert rpc.request.session_id == "session-1"
    assert rpc.request.window.seconds == 300
    assert rpc.request.window.nanos == 250_000_000
    assert metrics.requested_window == 300.25
    assert metrics.cpu.average_cores is None
    assert metrics.cpu.sample_count == 3
    assert metrics.cpu.observed_duration == 60.5
    assert metrics.cpu.coverage_percent == 75.5
    assert metrics.cpu.first_sample_at == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
    assert metrics.cpu.last_sample_at == datetime.fromtimestamp(1_700_000_060, tz=timezone.utc)
    assert metrics.memory.average_bytes == 512
    assert metrics.memory.coverage_percent == 80.25
    assert metrics.memory.first_sample_at == datetime.fromtimestamp(1_700_000_010, tz=timezone.utc)
    assert metrics.memory.last_sample_at == datetime.fromtimestamp(1_700_000_070, tz=timezone.utc)


def test_session_metrics_runtime_descriptors_include_converter_fields():
    expected = {"coverage_percent", "first_sample_at", "last_sample_at"}

    assert expected <= set(pb.SessionCPUUsageAverage.DESCRIPTOR.fields_by_name)
    assert expected <= set(pb.SessionMemoryUsageAverage.DESCRIPTOR.fields_by_name)


def test_get_session_metrics_sends_negative_window_for_server_validation():
    rpc = MetricsRPC()
    Client(rpc=rpc).get_session_metrics("session-1", window=-60)

    assert rpc.request.window.seconds == -60


def test_sandbox_metrics_uses_its_session_id():
    rpc = MetricsRPC()
    client = Client(rpc=rpc)
    sandbox = client.get("session-1")
    metrics = sandbox.metrics()

    assert metrics.session_id == "session-1"
    assert rpc.request.session_id == "session-1"
