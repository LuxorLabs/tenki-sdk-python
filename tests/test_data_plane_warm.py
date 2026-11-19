from __future__ import annotations

import asyncio

from tenki_sandbox import data_plane
from tenki_sandbox.aio import data_plane as aio_data_plane


def test_warm_pools_only_https_candidates_once(monkeypatch) -> None:
    started = []
    monkeypatch.setattr(data_plane.threading.Thread, "start", lambda self: started.append(self))
    before = dict(data_plane._shared_channels)
    try:
        data_plane.warm_data_plane_endpoints(
            ["https://warm.invalid", " http://plain.invalid ", "https://warm.invalid/", ""]
        )
        added = {k: v for k, v in data_plane._shared_channels.items() if k not in before}
        assert list(added) == [("warm.invalid:443", True)]
        assert added[("warm.invalid:443", True)][1] == 1
        assert len(started) == 1

        channel, release = data_plane._acquire_shared_channel("https://warm.invalid")
        assert channel is added[("warm.invalid:443", True)][0]
        release()
    finally:
        for key, shared in list(data_plane._shared_channels.items()):
            if key not in before:
                shared[0].close()
                del data_plane._shared_channels[key]


def test_async_warm_holds_the_channel_then_releases(monkeypatch) -> None:
    monkeypatch.setattr(aio_data_plane, "_DATA_PLANE_WARM_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(aio_data_plane, "_DATA_PLANE_WARM_HOLD_SECONDS", 0.05)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        key = (loop, "warm.invalid:443", True)
        aio_data_plane.warm_data_plane_endpoints(["https://warm.invalid", "http://plain.invalid"])
        assert aio_data_plane._shared_channels[key][1] == 1
        assert not any(k[1] == "plain.invalid:80" for k in aio_data_plane._shared_channels)
        await asyncio.gather(*list(aio_data_plane._warm_tasks))
        assert key not in aio_data_plane._shared_channels

    asyncio.run(scenario())


def test_async_warm_releases_the_channel_when_cancelled(monkeypatch) -> None:
    monkeypatch.setattr(aio_data_plane, "_DATA_PLANE_WARM_TIMEOUT_SECONDS", 30.0)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        key = (loop, "warm.invalid:443", True)
        aio_data_plane.warm_data_plane_endpoints(["https://warm.invalid"])
        tasks = list(aio_data_plane._warm_tasks)
        await asyncio.sleep(0)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert key not in aio_data_plane._shared_channels

    asyncio.run(scenario())


def test_warm_caps_the_endpoints_it_opens(monkeypatch) -> None:
    started = []
    monkeypatch.setattr(data_plane.threading.Thread, "start", lambda self: started.append(self))
    before = dict(data_plane._shared_channels)
    try:
        data_plane.warm_data_plane_endpoints([f"https://host-{i}.invalid" for i in range(12)])
        assert len(started) == 8
    finally:
        for key, shared in list(data_plane._shared_channels.items()):
            if key not in before:
                shared[0].close()
                del data_plane._shared_channels[key]
