"""Async fake RPCs for AsyncClient/AsyncSandbox tests.

Mirrors the sync FakeRPC contract but with `async def` unary methods and
streaming methods that return async generators (matching how the async
transport distinguishes them).
"""

from __future__ import annotations

from tenki_sandbox._pb import pb, template_pb


class FakeRPCError(Exception):
    def __init__(self, code_name: str, details: str = ""):
        super().__init__(details or code_name)
        self._code_name = code_name
        self._details = details or code_name

    def code(self):
        return type("Code", (), {"name": self._code_name})()

    def details(self):
        return self._details


def session(state=pb.SESSION_STATE_RUNNING, sid="session-1"):
    return pb.SandboxSession(
        id=sid,
        state=state,
        owner_type="SERVICE",
        owner_id="self",
        name="test",
        project_id="proj-1",
        workspace_id="ws-1",
    )


class FakeControlRPC:
    """Async control-plane fake. Data-plane bootstrap disabled by default."""

    def __init__(self, *, data_plane_endpoint: str = ""):
        self.requests: dict[str, list] = {}
        self.get_states = [pb.SESSION_STATE_RUNNING]
        self.wait_states = [pb.SESSION_STATE_RUNNING]
        self.wait_unimplemented = False
        self._data_plane_endpoint = data_plane_endpoint
        self.create_state = pb.SESSION_STATE_CREATING
        self.create_route_status = pb.DATA_PLANE_ROUTE_STATUS_UNSPECIFIED
        self.create_credential = None
        self.wait_data_plane_endpoint = ""
        self.wait_data_plane_credential = None
        self.wait_route_status = pb.DATA_PLANE_ROUTE_STATUS_UNSPECIFIED
        self.closed = False

    def _record(self, name, req):
        self.requests.setdefault(name, []).append(req)

    async def create_session(self, req):
        self._record("create_session", req)
        resp = pb.CreateSessionResponse(
            session=session(self.create_state),
            data_plane_endpoint=self._data_plane_endpoint,
            route_status=self.create_route_status,
        )
        if self.create_credential is not None:
            resp.credential.CopyFrom(self.create_credential)
        return resp

    async def get_session(self, req):
        self._record("get_session", req)
        state = self.get_states.pop(0) if self.get_states else pb.SESSION_STATE_RUNNING
        return pb.GetSessionResponse(session=session(state))

    def wait_session(self, req, **kwargs):
        self._record("wait_session", req)
        states = list(self.wait_states)
        unimplemented = self.wait_unimplemented
        endpoint = self.wait_data_plane_endpoint
        credential = self.wait_data_plane_credential
        route_status = self.wait_route_status

        async def gen():
            if unimplemented:
                raise FakeRPCError("UNIMPLEMENTED")
            for state in states:
                resp = pb.WaitSessionResponse(
                    session=session(state),
                    data_plane_endpoint=endpoint,
                    route_status=route_status,
                )
                if credential is not None:
                    resp.credential.CopyFrom(credential)
                yield resp

        return gen()

    async def list_sessions(self, req):
        self._record("list_sessions", req)
        return pb.ListSessionsResponse(sessions=[session()])

    async def terminate_session(self, req):
        self._record("terminate_session", req)
        return pb.TerminateSessionResponse(session=session(pb.SESSION_STATE_TERMINATED))

    async def pause_session(self, req):
        self._record("pause_session", req)
        return pb.PauseSessionResponse(session=session(pb.SESSION_STATE_PAUSED))

    async def resume_session(self, req):
        self._record("resume_session", req)
        return pb.ResumeSessionResponse(session=session(pb.SESSION_STATE_RUNNING))

    async def who_am_i(self, req):
        self._record("who_am_i", req)
        return pb.WhoAmIResponse(owner_type="USER", owner_id="user-1")

    async def build_template(self, req):
        self._record("build_template", req)
        return template_pb.BuildTemplateResponse(
            build=template_pb.TemplateBuild(
                id="build-1",
                template_id=req.template_id,
                state=template_pb.TEMPLATE_BUILD_STATE_READY,
            )
        )

    async def get_workspace_sandbox_usage(self, req):
        self._record("get_workspace_sandbox_usage", req)
        return pb.GetWorkspaceSandboxUsageResponse(
            limits=[
                pb.WorkspaceSandboxUsageLimit(
                    key="active_sessions",
                    label="Active sessions",
                    unit=pb.SANDBOX_USAGE_UNIT_COUNT,
                    current=2,
                    max=10,
                    help_text="live VMs",
                ),
                pb.WorkspaceSandboxUsageLimit(
                    key="snapshot_storage",
                    label="Snapshot storage",
                    unit=pb.SANDBOX_USAGE_UNIT_BYTES,
                    current=1024,
                    # max unset -> unlimited
                ),
            ]
        )

    async def update_session(self, req):
        self._record("update_session", req)
        return pb.UpdateSessionResponse(session=session())

    async def update_snapshot(self, req):
        self._record("update_snapshot", req)
        return pb.UpdateSnapshotResponse(snapshot=pb.Snapshot(id="snap-1"))

    async def attach_volume(self, req):
        self._record("attach_volume", req)
        return pb.AttachVolumeResponse()

    async def detach_volume(self, req):
        self._record("detach_volume", req)
        return pb.DetachVolumeResponse()

    def close(self):
        self.closed = True
