import pytest

from tenki_sandbox import DiskUsage  # re-exported from tenki_sandbox.disk

TOTAL = 5 * 1024**3


@pytest.mark.parametrize(
    ("disk", "exhausted"),
    [
        pytest.param(
            DiskUsage(total_bytes=0, available_bytes=0, min_available_bytes=0),
            False,
            id="unreported-by-guest",
        ),
        pytest.param(
            DiskUsage(
                total_bytes=TOTAL,
                available_bytes=2 * 1024**3,
                min_available_bytes=2 * 1024**3,
            ),
            False,
            id="healthy-run",
        ),
        pytest.param(
            # npm hits ENOSPC while unpacking, warns, exits 0, and frees enough
            # on the way out that the exit-time reading looks survivable. Only
            # the low-water mark shows the run was defeated.
            DiskUsage(
                total_bytes=TOTAL,
                available_bytes=214 * 1024**2,
                min_available_bytes=0,
            ),
            True,
            id="recovered-before-exit",
        ),
        pytest.param(
            # Allocators give up short of the last byte, so a disk that is full
            # from the workload's point of view still reports a few MiB free.
            DiskUsage(
                total_bytes=TOTAL,
                available_bytes=4 * 1024**2,
                min_available_bytes=4 * 1024**2,
            ),
            True,
            id="near-zero-floor",
        ),
        pytest.param(
            DiskUsage(
                total_bytes=TOTAL,
                available_bytes=64 * 1024**2,
                min_available_bytes=64 * 1024**2,
            ),
            False,
            id="above-the-floor",
        ),
    ],
)
def test_exhausted(disk: DiskUsage, exhausted: bool) -> None:
    assert disk.exhausted is exhausted


def test_used_bytes_matches_df() -> None:
    assert DiskUsage(total_bytes=0, available_bytes=0, min_available_bytes=0).used_bytes == 0

    # Real numbers from a 5 GiB sandbox: df reports 1963040768 used and
    # 2948128768 available, with 285212672 held back as the reserve. Counting
    # the reserve as used would report 43% where df prints 40%.
    disk = DiskUsage(
        total_bytes=5196382208,
        available_bytes=2948128768,
        min_available_bytes=2948128768,
        free_bytes=2948128768 + 285212672,
    )
    assert disk.used_bytes == 1963040768
    assert 39.5 < disk.used_percent < 40.5


def test_older_guest_without_free_bytes_overstates_rather_than_invents() -> None:
    disk = DiskUsage(
        total_bytes=TOTAL,
        available_bytes=2 * 1024**3,
        min_available_bytes=2 * 1024**3,
    )
    assert disk.used_bytes == 3 * 1024**3
    assert 59.5 < disk.used_percent < 60.5


def test_used_percent_guards_empty_disk() -> None:
    assert DiskUsage(total_bytes=0, available_bytes=0, min_available_bytes=0).used_percent == 0.0


def test_command_failed_error_names_the_disk() -> None:
    from tenki_sandbox import CommandResult
    from tenki_sandbox.errors import CommandFailedError

    # pip's own output says ENOENT and never mentions disk, so check=True
    # callers only learn the cause if the raised error says it.
    result = CommandResult(
        argv=["pip", "install", "torch"],
        exit_code=1,
        disk=DiskUsage(
            total_bytes=5196382208,
            available_bytes=0,
            min_available_bytes=0,
            free_bytes=0,
        ),
    )
    message = str(CommandFailedError(result))

    assert "exit_code=1" in message
    assert "ran out of space" in message
    assert "4.84 GiB total" in message
    assert "disk_size_gb" in message


def test_command_failed_error_stays_quiet_on_a_healthy_disk() -> None:
    from tenki_sandbox import CommandResult
    from tenki_sandbox.errors import CommandFailedError

    result = CommandResult(
        argv=["grep", "nope"],
        exit_code=1,
        disk=DiskUsage(
            total_bytes=TOTAL,
            available_bytes=2 * 1024**3,
            min_available_bytes=2 * 1024**3,
            free_bytes=2 * 1024**3,
        ),
    )
    assert "ran out of space" not in str(CommandFailedError(result))


def test_command_failed_error_without_disk_stats() -> None:
    from tenki_sandbox import CommandResult
    from tenki_sandbox.errors import CommandFailedError

    # Older guest: no disk on the frame, message unchanged.
    result = CommandResult(argv=["false"], exit_code=1)
    assert str(CommandFailedError(result)) == "command failed: exit_code=1 argv='false'"
