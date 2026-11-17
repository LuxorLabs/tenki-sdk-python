from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from tenki_sandbox import AsyncClient, Client, TailnetAttachment, TailnetAuthKey, TailnetPort
from tenki_sandbox._pb import pb


@pytest.fixture
def old_tailnet_server():
    deadlines = []

    def create(request, context):
        return pb.CreateSessionResponse(
            session=pb.SandboxSession(id="old-api-session", state=pb.SESSION_STATE_RUNNING)
        )

    def terminate(request, context):
        assert request.session_id == "old-api-session"
        deadlines.append(context.time_remaining())
        return pb.TerminateSessionResponse()

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "tenki.sandbox.v1.SandboxService",
            {
                "CreateSession": grpc.unary_unary_rpc_method_handler(
                    create, request_deserializer=pb.CreateSessionRequest.FromString,
                    response_serializer=pb.CreateSessionResponse.SerializeToString,
                ),
                "TerminateSession": grpc.unary_unary_rpc_method_handler(
                    terminate, request_deserializer=pb.TerminateSessionRequest.FromString,
                    response_serializer=pb.TerminateSessionResponse.SerializeToString,
                ),
            },
        ),))
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"http://127.0.0.1:{port}", deadlines
        finally:
            server.stop(0).wait()


def test_sync_tailnet_cleanup_sets_real_transport_deadline(old_tailnet_server):
    endpoint, deadlines = old_tailnet_server
    with Client(base_url=endpoint, auth_token="tk_test", timeout=60) as client:
        with pytest.raises(RuntimeError, match="session terminated"):
            client.create(wait=False, tailnet=TailnetAttachment(
                auth_key=TailnetAuthKey("sentinel"), exposed_ports=(TailnetPort(6081, "udp"),)
            ))
    assert len(deadlines) == 1
    assert 0 < deadlines[0] <= 10.1


@pytest.mark.asyncio
async def test_async_tailnet_cleanup_sets_real_transport_deadline(old_tailnet_server):
    endpoint, deadlines = old_tailnet_server
    async with AsyncClient(base_url=endpoint, auth_token="tk_test", timeout=60) as client:
        with pytest.raises(RuntimeError, match="session terminated"):
            await client.create(wait=False, tailnet=TailnetAttachment(
                auth_key=TailnetAuthKey("sentinel"), exposed_ports=(TailnetPort(6081, "udp"),)
            ))
    assert len(deadlines) == 1
    assert 0 < deadlines[0] <= 10.1
