from __future__ import annotations

from tenki_sandbox import CIRunnerSize, CIRunnerSizes, Client
from tenki_sandbox._pb import pb


class FakeRPC:
    def __init__(self) -> None:
        self.request = None

    def list_ci_runner_sizes(self, req):
        self.request = req
        resp = pb.ListCIRunnerSizesResponse(default_runs_on_label="tenki-small-2c-4g")
        resp.sizes.append(pb.CIRunnerSize(runs_on_label="tenki-small-2c-4g", cpu=2, memory=4))
        resp.sizes.append(pb.CIRunnerSize(runs_on_label="tenki-medium-4c-8g", cpu=4, memory=8))
        return resp


def test_list_ci_runner_sizes_uses_the_keys_workspace_and_maps_each_size():
    rpc = FakeRPC()

    sizes = Client(rpc=rpc).list_ci_runner_sizes()

    assert not rpc.request.HasField("workspace_id")
    assert sizes == CIRunnerSizes(
        sizes=(
            CIRunnerSize(runs_on_label="tenki-small-2c-4g", cpu=2, memory_gib=4),
            CIRunnerSize(runs_on_label="tenki-medium-4c-8g", cpu=4, memory_gib=8),
        ),
        default_runs_on_label="tenki-small-2c-4g",
    )
