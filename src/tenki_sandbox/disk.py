"""Sandbox filesystem usage.

Kept in its own module so the parity gate can pair it with the Go and
TypeScript equivalents; the thresholds and df semantics have to agree across
all three or callers get different answers per language.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Free-space floor under which a run is treated as having exhausted the disk.
#: Not zero: allocators fail short of the last byte, and ext4 keeps a root
#: reserve the workload cannot touch, so a workload-visible "full" disk still
#: reports a small non-zero available figure.
_DISK_EXHAUSTION_AVAILABLE_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class DiskUsage:
    """Sandbox filesystem state observed while a command ran."""

    total_bytes: int
    #: Free space when the command exited, excluding the reserve: what the
    #: workload itself can still write.
    available_bytes: int
    #: Least free space seen while the command ran. This is the field that
    #: identifies a run defeated by a full disk: pip, npm and containerd all
    #: delete their partial downloads after hitting ENOSPC, so
    #: ``available_bytes`` recovers before the command exits and only this
    #: stays low. ``Sandbox.disk_usage()`` takes a single sample, so there it
    #: equals ``available_bytes``.
    min_available_bytes: int

    @property
    def exhausted(self) -> bool:
        """Whether the filesystem ran out of usable space during the command.

        Deliberately independent of exit status: npm hits ENOSPC while
        unpacking, prints it as a warning and still exits 0, leaving a broken
        install behind, so a caller that only checks this on failure misses the
        case that is hardest to diagnose.
        """
        if self.total_bytes <= 0:
            return False
        return self.min_available_bytes < _DISK_EXHAUSTION_AVAILABLE_BYTES

    #: Counts the filesystem reserve that ``available_bytes`` excludes.
    #: ``free_bytes - available_bytes`` is the reserve itself, which is why a
    #: root-owned write can still land on a filesystem the workload sees as
    #: full. Zero from a guest too old to report it.
    free_bytes: int = 0

    @property
    def used_bytes(self) -> int:
        """Space consumed, counted as df does: total minus free.

        The reserve is neither used nor available. A guest too old to report
        ``free_bytes`` falls back to treating the reserve as used, which
        overstates usage rather than inventing headroom.
        """
        if self.total_bytes <= 0:
            return 0
        free = self.free_bytes if self.free_bytes > 0 else self.available_bytes
        return self.total_bytes - free

    @property
    def used_percent(self) -> float:
        """Usage as df reports it, against usable capacity rather than raw total."""
        used = self.used_bytes
        usable = used + self.available_bytes
        if usable <= 0:
            return 0.0
        return used / usable * 100
