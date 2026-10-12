from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import grpc
import pytest
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.data_plane import AsyncGrpcDataPlaneRPC
from tenki_sandbox.data_plane import DataPlaneCredentialManager, GrpcDataPlaneRPC

_CASES = [
    ("run", "Run", pb.RunRequest, "start"),
    ("dial", "Dial", pb.DialRequest, "open"),
    ("host_port_tunnel", "HostPortTunnel", pb.HostPortTunnelRequest, "open"),
    ("write_file_stream", "WriteFileStream", pb.WriteFileStreamRequest, "start"),
]


@pytest.mark.parametrize("method,rpc_name,frame_type,opening", _CASES)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_stream_session_binding_over_grpc(
    method, rpc_name, frame_type, opening, asynchronous
):
    request_type = getattr(pb, f"SandboxSessionDataPlaneService{rpc_name}Request")
    response_type = getattr(pb, f"SandboxSessionDataPlaneService{rpc_name}Response")
    seen = []

    def consume(requests, context):
        assert (
            dict(context.invocation_metadata())["x-tenki-session-cert"]
            == "stream-credential"
        )
        for request in requests:
            seen.append(getattr(request.frame, opening).session_id)
        return response_type()

    def stream(requests, context):
        yield consume(requests, context)

    handler = (
        grpc.stream_unary_rpc_method_handler
        if method == "write_file_stream"
        else grpc.stream_stream_rpc_method_handler
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "tenki.sandbox.v1.SandboxSessionDataPlaneService",
                    {
                        rpc_name: handler(
                            consume if method == "write_file_stream" else stream,
                            request_deserializer=request_type.FromString,
                            response_serializer=response_type.SerializeToString,
                        )
                    },
                ),
            )
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        endpoint = f"http://127.0.0.1:{port}"
        credentials = DataPlaneCredentialManager(
            SimpleNamespace(id="sess-1"),
            pb.SessionCredential(credential="stream-credential"),
            endpoint,
        )

        def frames(ids):
            return iter(frame_type(**{opening: {"session_id": value}}) for value in ids)

        async def async_scenario():
            rpc = AsyncGrpcDataPlaneRPC(endpoint, credentials, "sess-1")

            async def call(ids):
                async def requests():
                    for frame in frames(ids):
                        yield frame

                result = getattr(rpc, method)(requests(), timeout=3)
                if method == "write_file_stream":
                    await result
                else:
                    async for _ in result:
                        pass

            try:
                await call(["sess-1"])
                assert seen == ["sess-1"]
                for ids in [["other-session"], ["sess-1", "other-session"]]:
                    with pytest.raises((grpc.RpcError, asyncio.CancelledError)):
                        await call(ids)
                    assert "other-session" not in seen
            finally:
                await rpc.close()

        try:
            if asynchronous:
                asyncio.run(async_scenario())
            else:
                rpc = GrpcDataPlaneRPC(endpoint, credentials, "sess-1")

                def call(ids):
                    result = getattr(rpc, method)(frames(ids), timeout=3)
                    if method != "write_file_stream":
                        list(result)

                try:
                    call(["sess-1"])
                    assert seen == ["sess-1"]
                    for ids in [["other-session"], ["sess-1", "other-session"]]:
                        with pytest.raises(grpc.RpcError):
                            call(ids)
                        assert "other-session" not in seen
                finally:
                    rpc.close()
        finally:
            credentials.stop()
            server.stop(0).wait()
