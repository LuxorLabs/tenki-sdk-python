from __future__ import annotations

from .errors import InvalidResourceConfigError

MIN_CPU_CORES = 1
MAX_CPU_CORES = 16
MIN_CREATE_MEMORY_MB = 128
MIN_PROVISION_MEMORY_MB = 512
MAX_MEMORY_MB = 65536
MIN_DISK_SIZE_GB = 5
MAX_DISK_SIZE_GB = 100


def validate_create_resources(
    cpu_cores: int | None,
    memory_mb: int | None,
    disk_size_gb: int | None,
) -> None:
    if cpu_cores is not None:
        _validate_cpu_cores(cpu_cores)
    if memory_mb is not None:
        _validate_memory_mb(memory_mb, MIN_CREATE_MEMORY_MB)
    if disk_size_gb is not None:
        _validate_disk_size_gb(disk_size_gb)


def validate_template_resources(
    cpu_cores: int | None,
    memory_mb: int | None,
    disk_size_gb: int | None,
) -> None:
    if cpu_cores is not None and not _is_template_default(cpu_cores):
        _validate_cpu_cores(cpu_cores)
    if memory_mb is not None and not _is_template_default(memory_mb):
        _validate_memory_mb(memory_mb, MIN_PROVISION_MEMORY_MB)
    if disk_size_gb is not None and not _is_template_default(disk_size_gb):
        _validate_disk_size_gb(disk_size_gb)


def _validate_cpu_cores(cpu_cores: int) -> None:
    if _is_int(cpu_cores) and MIN_CPU_CORES <= cpu_cores <= MAX_CPU_CORES:
        return
    raise InvalidResourceConfigError(
        f"cpu_cores must be between {MIN_CPU_CORES} and {MAX_CPU_CORES}"
    )


def _validate_memory_mb(memory_mb: int, min_memory_mb: int) -> None:
    if not _is_int(memory_mb) or not min_memory_mb <= memory_mb <= MAX_MEMORY_MB:
        raise InvalidResourceConfigError(
            f"memory_mb must be between {min_memory_mb} and {MAX_MEMORY_MB}"
        )
    if memory_mb % 2 != 0:
        raise InvalidResourceConfigError("memory_mb must be aligned to 2 MiB")


def _validate_disk_size_gb(disk_size_gb: int) -> None:
    if _is_int(disk_size_gb) and MIN_DISK_SIZE_GB <= disk_size_gb <= MAX_DISK_SIZE_GB:
        return
    raise InvalidResourceConfigError(
        f"disk_size_gb must be between {MIN_DISK_SIZE_GB} and {MAX_DISK_SIZE_GB}"
    )


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_template_default(value: object) -> bool:
    return _is_int(value) and value == 0
