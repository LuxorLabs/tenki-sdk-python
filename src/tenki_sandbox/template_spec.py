"""Immutable typed template build spec builder (tenki.template.v1).

Every fluent method returns a new ``TemplateSpec``; instances are never
mutated in place. The builder emits the shared ``TemplateBuildSpec`` proto,
supports strict authored-JSON import/export, and performs aggregate local
validation mirroring the server's protovalidate rules. Canonical
normalization and the authoritative ``spec_hash`` remain server-owned.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Callable

from google.protobuf import json_format

from ._pb import template_pb
from .errors import TemplateSpecValidationError

SPEC_VERSION = "tenki.template.v1"

_DEFAULT_BASE_IMAGE = "sandbox"
_DEFAULT_WORKDIR = "/home/tenki"
_DEFAULT_GIT_WORKDIR = "/home/tenki/app"
_DEFAULT_RUN_TIMEOUT = 1800
_DEFAULT_STOP_GRACE = 30
_DEFAULT_READY_TIMEOUT = 60

_CHECKOUT_MODES = {
    "contents": template_pb.TEMPLATE_CHECKOUT_MODE_CONTENTS,
    "directory": template_pb.TEMPLATE_CHECKOUT_MODE_DIRECTORY,
}
_RUN_AT = {
    "boot": template_pb.TEMPLATE_RUNTIME_RUN_AT_BOOT,
    "build": template_pb.TEMPLATE_RUNTIME_RUN_AT_BUILD,
    "manual": template_pb.TEMPLATE_RUNTIME_RUN_AT_MANUAL,
}
_RESTART_POLICIES = {
    "never": template_pb.TEMPLATE_RESTART_POLICY_NEVER,
    "on_failure": template_pb.TEMPLATE_RESTART_POLICY_ON_FAILURE,
    "always": template_pb.TEMPLATE_RESTART_POLICY_ALWAYS,
}
_SNAPSHOT_MODES = {
    "filesystem": template_pb.TEMPLATE_SNAPSHOT_MODE_FILESYSTEM,
    "memory": template_pb.TEMPLATE_SNAPSHOT_MODE_MEMORY,
}

_AUTHORED_ENUM_FIELDS = {
    ("context", "checkout", "mode"): {
        "contents": "TEMPLATE_CHECKOUT_MODE_CONTENTS",
        "directory": "TEMPLATE_CHECKOUT_MODE_DIRECTORY",
    },
    ("runtime", "runAt"): {
        "boot": "TEMPLATE_RUNTIME_RUN_AT_BOOT",
        "build": "TEMPLATE_RUNTIME_RUN_AT_BUILD",
        "manual": "TEMPLATE_RUNTIME_RUN_AT_MANUAL",
    },
    ("runtime", "run_at"): {
        "boot": "TEMPLATE_RUNTIME_RUN_AT_BOOT",
        "build": "TEMPLATE_RUNTIME_RUN_AT_BUILD",
        "manual": "TEMPLATE_RUNTIME_RUN_AT_MANUAL",
    },
    ("runtime", "restartPolicy"): {
        "never": "TEMPLATE_RESTART_POLICY_NEVER",
        "on-failure": "TEMPLATE_RESTART_POLICY_ON_FAILURE",
        "always": "TEMPLATE_RESTART_POLICY_ALWAYS",
    },
    ("runtime", "restart_policy"): {
        "never": "TEMPLATE_RESTART_POLICY_NEVER",
        "on-failure": "TEMPLATE_RESTART_POLICY_ON_FAILURE",
        "always": "TEMPLATE_RESTART_POLICY_ALWAYS",
    },
    ("runtime", "snapshotMode"): {
        "filesystem": "TEMPLATE_SNAPSHOT_MODE_FILESYSTEM",
        "memory": "TEMPLATE_SNAPSHOT_MODE_MEMORY",
    },
    ("runtime", "snapshot_mode"): {
        "filesystem": "TEMPLATE_SNAPSHOT_MODE_FILESYSTEM",
        "memory": "TEMPLATE_SNAPSHOT_MODE_MEMORY",
    },
}

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_HTTP_READY_URL_RE = re.compile(
    r"^https?://(localhost|127\.0\.0\.1|\[::1\])(:[0-9]+)?(/.*)?$"
)
_PACKAGE_RE = re.compile(r"^[^-\x00][^\x00]*$")


def _enum(value: str, table: Mapping[str, int], field: str) -> int:
    try:
        return table[value]
    except KeyError:
        raise ValueError(
            f"{field} must be one of {sorted(table)}, got {value!r}"
        ) from None


def _normalize_authored_enums(
    value: Any, *, to_authored: bool, path: tuple[Any, ...] = ()
) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _normalize_authored_enums(
                item, to_authored=to_authored, path=path + (key,)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _normalize_authored_enums(item, to_authored=to_authored, path=path)
            for item in value
        ]
    if not isinstance(value, str):
        return value
    values = _AUTHORED_ENUM_FIELDS.get(path)
    if values is None:
        return value
    if not to_authored:
        return values.get(value, value)
    return next(
        (short for short, protobuf_name in values.items() if protobuf_name == value),
        value,
    )


def _reject_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field {key!r}")
        result[key] = value
    return result


def _resource_id(value: Any, kind: str) -> str:
    candidate = value if isinstance(value, str) else getattr(value, "id", None)
    if not isinstance(candidate, str) or not candidate.strip():
        raise ValueError(f"{kind} requires an id or a resource object with one")
    return candidate.strip()


def _image_ref(value: Any) -> str:
    if isinstance(value, str):
        ref = value.strip()
    else:
        ref = (getattr(value, "digest_ref", None) or getattr(value, "ref", "") or "").strip()
    if not ref:
        raise ValueError("image requires a non-empty ref or an Image object")
    return ref


def _set_command(msg, command: Any, field: str) -> None:
    if isinstance(command, str):
        if not command:
            raise ValueError(f"{field}: command cannot be empty")
        del msg.argv[:]
        msg.command = command
    elif isinstance(command, Sequence):
        argv = [str(part) for part in command]
        if not argv:
            raise ValueError(f"{field}: argv cannot be empty")
        del msg.argv[:]
        msg.argv.extend(argv)
        msg.command = ""
    else:
        raise TypeError(f"{field}: expected a shell string or argv sequence")


def _set_check(check, item: Any, field: str) -> None:
    if isinstance(item, int) and not isinstance(item, bool):
        check.port = item
        return
    if not isinstance(item, Mapping) or len(item) != 1:
        raise ValueError(
            f"{field}: each check must be a port int or a mapping with exactly"
            " one of 'port', 'http', 'exec'"
        )
    ((kind, value),) = item.items()
    if kind == "port":
        check.port = int(value)
    elif kind == "http":
        if isinstance(value, str):
            check.http.url = value
        elif isinstance(value, Mapping):
            check.http.url = value.get("url", "")
            codes = value.get("success_status_codes") or ()
            check.http.success_status_codes.extend(int(code) for code in codes)
        else:
            raise ValueError(f"{field}: 'http' check must be a url or mapping")
    elif kind == "exec":
        if isinstance(value, (str, Sequence)) and not isinstance(value, Mapping):
            _set_command(check.exec, value, field)
        elif isinstance(value, Mapping):
            if "command" in value:
                _set_command(check.exec, value["command"], field)
            elif "argv" in value:
                _set_command(check.exec, list(value["argv"]), field)
            else:
                raise ValueError(f"{field}: 'exec' check requires command or argv")
            timeout = value.get("timeout_seconds", value.get("timeout"))
            if timeout is not None:
                check.exec.timeout_seconds = int(timeout)
        else:
            raise ValueError(f"{field}: 'exec' check must be a command or mapping")
    else:
        raise ValueError(f"{field}: unknown check kind {kind!r}")


def _set_ready_when(msg, value: Any, field: str = "ready_when") -> None:
    if isinstance(value, Mapping):
        checks = value.get("checks") or ()
        timeout = value.get("timeout_seconds", value.get("timeout", _DEFAULT_READY_TIMEOUT))
        poll = value.get("poll_interval_seconds", value.get("poll_interval"))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        checks = value
        timeout = _DEFAULT_READY_TIMEOUT
        poll = None
    else:
        raise TypeError(f"{field}: expected a list of checks or a mapping")
    msg.Clear()
    msg.timeout_seconds = int(timeout)
    if poll is not None:
        msg.poll_interval_seconds = int(poll)
    for item in checks:
        _set_check(msg.checks.add(), item, f"{field}.checks")


def _flatten_packages(packages: tuple) -> list[str]:
    if len(packages) == 1 and isinstance(packages[0], Sequence) and not isinstance(
        packages[0], str
    ):
        packages = tuple(packages[0])
    return [str(pkg) for pkg in packages]


class TemplateSpec:
    """Immutable local builder for the typed v1 template build spec.

    The default spec starts from the Tenki base image ``sandbox``. Local
    ``validate()`` rejects obvious errors; the server remains authoritative
    and owns canonical hashing (no local ``hash()``).
    """

    __slots__ = ("_proto",)

    def __init__(self) -> None:
        proto = template_pb.TemplateBuildSpec(spec_version=SPEC_VERSION)
        proto.base.image = _DEFAULT_BASE_IMAGE
        self._proto = proto

    @classmethod
    def _wrap(cls, proto) -> "TemplateSpec":
        spec = cls.__new__(cls)
        spec._proto = proto
        return spec

    def _evolve(self, mutate: Callable) -> "TemplateSpec":
        proto = template_pb.TemplateBuildSpec()
        proto.CopyFrom(self._proto)
        mutate(proto)
        return TemplateSpec._wrap(proto)

    # Base selection (exactly one of image / template / snapshot)

    def from_image(self, image: Any) -> "TemplateSpec":
        """Base on a Tenki-managed image ID or registry ref (or an Image)."""
        ref = _image_ref(image)

        def mutate(p):
            p.base.image = ref

        return self._evolve(mutate)

    def from_template(self, template: Any) -> "TemplateSpec":
        """Base on a parent template (resource object or ID)."""
        template_id = _resource_id(template, "from_template")

        def mutate(p):
            p.base.template_id = template_id

        return self._evolve(mutate)

    def from_snapshot(self, snapshot: Any) -> "TemplateSpec":
        """Base on a parent snapshot (resource object or ID)."""
        snapshot_id = _resource_id(snapshot, "from_snapshot")

        def mutate(p):
            p.base.snapshot_id = snapshot_id

        return self._evolve(mutate)

    # Context and environment

    def workdir(self, path: str) -> "TemplateSpec":
        def mutate(p):
            p.workdir = path

        return self._evolve(mutate)

    def with_git_context(
        self,
        repo: str,
        ref: str = "main",
        *,
        dest: str | None = None,
        mode: str | None = None,
        name: str | None = None,
        ignore: Sequence[str] | None = None,
    ) -> "TemplateSpec":
        """Use a backend-fetched Git repository as the build context.

        ``mode`` is ``"contents"`` (repo contents land in ``dest``) or
        ``"directory"`` (a child directory, optionally named ``name``).
        Copy steps are relative to this checked-out context.
        """
        mode_value = _enum(mode, _CHECKOUT_MODES, "mode") if mode is not None else None

        def mutate(p):
            p.ClearField("context")
            p.context.source.git.repo = repo
            p.context.source.git.ref = ref
            if dest is not None:
                p.context.checkout.dest = dest
            if mode_value is not None:
                p.context.checkout.mode = mode_value
            if name is not None:
                p.context.checkout.name = name
            if ignore:
                p.context.ignore.extend(ignore)

        return self._evolve(mutate)

    def build_env(self, env: Mapping[str, str]) -> "TemplateSpec":
        """Merge persisted (non-secret) build-time environment variables."""

        def mutate(p):
            p.build.env.update(dict(env))

        return self._evolve(mutate)

    def runtime_env(self, env: Mapping[str, str]) -> "TemplateSpec":
        """Merge persisted runtime (process-scoped) environment variables."""

        def mutate(p):
            p.runtime.env.update(dict(env))

        return self._evolve(mutate)

    # Ordered build steps

    def run(
        self,
        command: str | Sequence[str],
        *,
        name: str | None = None,
        workdir: str | None = None,
        timeout: int | None = None,
    ) -> "TemplateSpec":
        """Append a run step: a shell string (``sh -lc``) or an argv list."""

        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            _set_command(step.run, command, "run")
            if workdir is not None:
                step.run.workdir = workdir
            if timeout is not None:
                step.run.timeout_seconds = int(timeout)

        return self._evolve(mutate)

    def copy(self, src: str, dest: str, *, name: str | None = None) -> "TemplateSpec":
        """Copy from the Git context (relative ``src``) to absolute ``dest``."""

        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.copy.src = src
            step.copy.dest = dest

        return self._evolve(mutate)

    def write_file(
        self,
        path: str,
        content: str | bytes,
        *,
        mode: int | None = None,
        name: str | None = None,
    ) -> "TemplateSpec":
        data = content.encode() if isinstance(content, str) else bytes(content)

        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.write_file.path = path
            step.write_file.content = data
            if mode is not None:
                step.write_file.mode = mode

        return self._evolve(mutate)

    def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        mode: int | None = None,
        name: str | None = None,
    ) -> "TemplateSpec":
        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.mkdir.path = path
            step.mkdir.parents = parents
            if mode is not None:
                step.mkdir.mode = mode

        return self._evolve(mutate)

    def remove(
        self, path: str, *, recursive: bool = False, name: str | None = None
    ) -> "TemplateSpec":
        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.remove.path = path
            step.remove.recursive = recursive

        return self._evolve(mutate)

    def rename(self, src: str, dest: str, *, name: str | None = None) -> "TemplateSpec":
        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.rename.src = src
            step.rename.dest = dest

        return self._evolve(mutate)

    def symlink(
        self, target: str, path: str, *, name: str | None = None
    ) -> "TemplateSpec":
        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            step.symlink.target = target
            step.symlink.path = path

        return self._evolve(mutate)

    def apt(self, *packages, name: str | None = None) -> "TemplateSpec":
        return self._package_step("apt", packages, name)

    def pip(self, *packages, name: str | None = None) -> "TemplateSpec":
        return self._package_step("pip", packages, name)

    def npm(self, *packages, name: str | None = None) -> "TemplateSpec":
        return self._package_step("npm", packages, name)

    def bun(self, *packages, name: str | None = None) -> "TemplateSpec":
        return self._package_step("bun", packages, name)

    def _package_step(self, kind: str, packages: tuple, name: str | None) -> "TemplateSpec":
        pkgs = _flatten_packages(packages)

        def mutate(p):
            step = p.steps.add()
            if name is not None:
                step.name = name
            getattr(step, kind).packages.extend(pkgs)
            getattr(step, kind).SetInParent()

        return self._evolve(mutate)

    # Runtime

    def start(
        self,
        command: str | Sequence[str],
        *,
        run_at: str | None = None,
        workdir: str | None = None,
        restart_policy: str | None = None,
        ready_when: Any | None = None,
        snapshot_mode: str | None = None,
        stop_grace_seconds: int | None = None,
    ) -> "TemplateSpec":
        """Declare a single start command as the runtime entrypoint.

        ``run_at`` is ``"boot"`` (default), ``"build"``, or ``"manual"``.
        ``ready_when`` accepts a list of checks (port int, ``{"http": url}``,
        ``{"exec": cmd}``) or a mapping with ``timeout`` / ``poll_interval`` /
        ``checks``. ``snapshot_mode`` (``"filesystem"``/``"memory"``) is only
        valid with ``run_at="build"``.
        """
        options = self._runtime_options(
            run_at, restart_policy, ready_when, snapshot_mode, stop_grace_seconds
        )

        def mutate(p):
            p.runtime.ClearField("entrypoint")
            _set_command(p.runtime.start, command, "start")
            if workdir is not None:
                p.runtime.start.workdir = workdir
            options(p.runtime)

        return self._evolve(mutate)

    def process_compose(
        self,
        config_path: str,
        *,
        workdir: str | None = None,
        env_files: Sequence[str] | None = None,
        run_at: str | None = None,
        restart_policy: str | None = None,
        ready_when: Any | None = None,
        snapshot_mode: str | None = None,
        stop_grace_seconds: int | None = None,
    ) -> "TemplateSpec":
        """Declare a process-compose supervisor as the runtime entrypoint."""
        options = self._runtime_options(
            run_at, restart_policy, ready_when, snapshot_mode, stop_grace_seconds
        )

        def mutate(p):
            p.runtime.ClearField("entrypoint")
            p.runtime.process_compose.config_path = config_path
            if workdir is not None:
                p.runtime.process_compose.workdir = workdir
            if env_files:
                p.runtime.process_compose.env_files.extend(env_files)
            options(p.runtime)

        return self._evolve(mutate)

    @staticmethod
    def _runtime_options(
        run_at, restart_policy, ready_when, snapshot_mode, stop_grace_seconds
    ) -> Callable:
        run_at_value = _enum(run_at, _RUN_AT, "run_at") if run_at is not None else None
        restart_value = (
            _enum(restart_policy, _RESTART_POLICIES, "restart_policy")
            if restart_policy is not None
            else None
        )
        snapshot_value = (
            _enum(snapshot_mode, _SNAPSHOT_MODES, "snapshot_mode")
            if snapshot_mode is not None
            else None
        )

        def apply(runtime):
            if run_at_value is not None:
                runtime.run_at = run_at_value
            if restart_value is not None:
                runtime.restart_policy = restart_value
            if snapshot_value is not None:
                runtime.snapshot_mode = snapshot_value
            if ready_when is not None:
                _set_ready_when(runtime.ready_when, ready_when)
            if stop_grace_seconds is not None:
                runtime.stop_grace_seconds = int(stop_grace_seconds)

        return apply

    def ready_when(
        self,
        checks: Sequence[Any] | Mapping[str, Any],
        *,
        timeout: int | None = None,
        poll_interval: int | None = None,
    ) -> "TemplateSpec":
        """Set the runtime readiness contract (all checks must pass once)."""
        value: Any = (
            dict(checks)
            if isinstance(checks, Mapping)
            else {"checks": list(checks)}
        )
        if timeout is not None:
            value["timeout"] = timeout
        if poll_interval is not None:
            value["poll_interval"] = poll_interval

        def mutate(p):
            _set_ready_when(p.runtime.ready_when, value)

        return self._evolve(mutate)

    def snapshot_mode(self, mode: str) -> "TemplateSpec":
        """Select ``"filesystem"`` or ``"memory"`` capture for build runtime."""
        mode_value = _enum(mode, _SNAPSHOT_MODES, "snapshot_mode")

        def mutate(p):
            p.runtime.snapshot_mode = mode_value

        return self._evolve(mutate)

    def resources(
        self,
        *,
        cpu_cores: int | None = None,
        memory_mb: int | None = None,
        disk_size_gb: int | None = None,
    ) -> "TemplateSpec":
        """Set default resources for the build VM and launched sandboxes."""

        def mutate(p):
            p.resources.SetInParent()
            if cpu_cores is not None:
                p.resources.cpu_cores = cpu_cores
            if memory_mb is not None:
                p.resources.memory_mb = memory_mb
            if disk_size_gb is not None:
                p.resources.disk_size_gb = disk_size_gb

        return self._evolve(mutate)

    # Import / export / validation

    def to_proto(self):
        proto = template_pb.TemplateBuildSpec()
        proto.CopyFrom(self._proto)
        return proto

    def to_dict(self) -> dict:
        """Authored JSON dict with camelCase fields and short enum values."""
        return _normalize_authored_enums(
            json_format.MessageToDict(self._proto), to_authored=True
        )

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_proto(cls, proto) -> "TemplateSpec":
        copied = template_pb.TemplateBuildSpec()
        copied.CopyFrom(proto)
        return cls._wrap(copied)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TemplateSpec":
        return cls._from_authored_dict(value)

    @classmethod
    def from_json(cls, text: str) -> "TemplateSpec":
        """Strict authored JSON import; unknown fields and enum values are rejected."""
        try:
            value = json.loads(text, object_pairs_hook=_reject_duplicate_fields)
        except (json.JSONDecodeError, ValueError) as err:
            raise TemplateSpecValidationError([str(err)]) from err
        return cls._from_authored_dict(value)

    @classmethod
    def _from_authored_dict(cls, value: Any) -> "TemplateSpec":
        if not isinstance(value, Mapping):
            raise TemplateSpecValidationError(
                ["template spec JSON must be an object"]
            )
        proto = template_pb.TemplateBuildSpec()
        try:
            json_format.ParseDict(
                _normalize_authored_enums(value, to_authored=False),
                proto,
                ignore_unknown_fields=False,
            )
        except json_format.ParseError as err:
            raise TemplateSpecValidationError([str(err)]) from err
        if proto.spec_version != SPEC_VERSION:
            raise TemplateSpecValidationError(
                [f"specVersion: must be {SPEC_VERSION!r}"]
            )
        return cls._wrap(proto)

    def validate(self) -> None:
        """Aggregate local validation; raises TemplateSpecValidationError.

        Mirrors the server's protovalidate rules for obvious errors after
        applying the server's semantic defaults. Server validation remains
        authoritative (stateful checks like base existence or Git access).
        """
        violations = _validate_spec(_with_defaults(self._proto))
        if violations:
            raise TemplateSpecValidationError(violations)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TemplateSpec) and self._proto == other._proto

    def __hash__(self) -> int:  # frozen-value semantics for use in sets/dicts
        return hash(self._proto.SerializeToString(deterministic=True))

    def __repr__(self) -> str:
        return f"TemplateSpec({json.dumps(self.to_dict(), sort_keys=True)})"


def _with_defaults(proto):
    """Mirror the server's applyTemplateSpecDefaults for local validation."""
    p = template_pb.TemplateBuildSpec()
    p.CopyFrom(proto)
    if not p.workdir:
        p.workdir = _DEFAULT_GIT_WORKDIR if p.HasField("context") else _DEFAULT_WORKDIR
    if p.HasField("context"):
        if not p.context.checkout.dest:
            p.context.checkout.dest = p.workdir
        if p.context.checkout.mode == template_pb.TEMPLATE_CHECKOUT_MODE_UNSPECIFIED:
            p.context.checkout.mode = template_pb.TEMPLATE_CHECKOUT_MODE_CONTENTS
    for step in p.steps:
        if step.WhichOneof("operation") == "run":
            if not step.run.workdir:
                step.run.workdir = p.workdir
            if not step.run.timeout_seconds:
                step.run.timeout_seconds = _DEFAULT_RUN_TIMEOUT
    if not p.HasField("runtime"):
        return p
    runtime = p.runtime
    if runtime.run_at == template_pb.TEMPLATE_RUNTIME_RUN_AT_UNSPECIFIED:
        runtime.run_at = template_pb.TEMPLATE_RUNTIME_RUN_AT_BOOT
    if runtime.WhichOneof("entrypoint") == "start" and not runtime.start.workdir:
        runtime.start.workdir = p.workdir
    if (
        runtime.WhichOneof("entrypoint") == "process_compose"
        and not runtime.process_compose.workdir
    ):
        runtime.process_compose.workdir = p.workdir
    if (
        runtime.run_at != template_pb.TEMPLATE_RUNTIME_RUN_AT_BUILD
        and runtime.restart_policy == template_pb.TEMPLATE_RESTART_POLICY_UNSPECIFIED
    ):
        runtime.restart_policy = template_pb.TEMPLATE_RESTART_POLICY_ON_FAILURE
    if (
        runtime.run_at == template_pb.TEMPLATE_RUNTIME_RUN_AT_BUILD
        and runtime.snapshot_mode == template_pb.TEMPLATE_SNAPSHOT_MODE_UNSPECIFIED
    ):
        runtime.snapshot_mode = template_pb.TEMPLATE_SNAPSHOT_MODE_FILESYSTEM
    if runtime.HasField("ready_when"):
        if not runtime.ready_when.poll_interval_seconds:
            runtime.ready_when.poll_interval_seconds = 1
        for check in runtime.ready_when.checks:
            if check.WhichOneof("check") == "exec" and not check.exec.timeout_seconds:
                check.exec.timeout_seconds = 10
    if not runtime.stop_grace_seconds:
        runtime.stop_grace_seconds = _DEFAULT_STOP_GRACE
    return p


def _abs_or_empty(path: str) -> bool:
    return path == "" or path.startswith("/")


def _context_relative(path: str) -> bool:
    return not path.startswith("/") and all(part != ".." for part in path.split("/"))


def _check_command_or_argv(out: list[str], msg, field: str) -> None:
    has_command = bool(msg.command)
    has_argv = len(msg.argv) > 0
    if has_command == has_argv:
        out.append(f"{field}: exactly one of command or argv is required")
    if len(msg.command) > 65536:
        out.append(f"{field}.command: must be at most 65536 characters")
    if len(msg.argv) > 1024:
        out.append(f"{field}.argv: at most 1024 items")
    if any(not part for part in msg.argv):
        out.append(f"{field}.argv: items cannot be empty")


def _check_path(
    out: list[str],
    value: str,
    field: str,
    *,
    required: bool = True,
    absolute: bool = True,
    max_len: int = 4096,
) -> None:
    if required and not value:
        out.append(f"{field}: is required")
        return
    if len(value) > max_len:
        out.append(f"{field}: must be at most {max_len} characters")
    if absolute and not _abs_or_empty(value):
        out.append(f"{field}: must be an absolute path")


def _validate_spec(p) -> list[str]:
    out: list[str] = []
    if p.spec_version != SPEC_VERSION:
        out.append(f"specVersion: must be {SPEC_VERSION!r}")

    base = p.base.WhichOneof("source")
    if base is None:
        out.append("base: exactly one base source is required")
    elif base == "image" and not p.base.image:
        out.append("base.image: is required")
    elif base == "template_id" and not _UUID_RE.match(p.base.template_id):
        out.append("base.template_id: must be a UUID")
    elif base == "snapshot_id" and not _UUID_RE.match(p.base.snapshot_id):
        out.append("base.snapshot_id: must be a UUID")

    _check_path(out, p.workdir, "workdir", required=False)

    if p.HasField("context"):
        ctx = p.context
        if not ctx.HasField("source") or ctx.source.WhichOneof("source") is None:
            out.append("context.source: a Git context source is required")
        else:
            git = ctx.source.git
            if not git.repo or len(git.repo) > 4096:
                out.append("context.source.git.repo: must be 1-4096 characters")
            if not git.ref or len(git.ref) > 1024:
                out.append("context.source.git.ref: must be 1-1024 characters")
        checkout = ctx.checkout
        _check_path(out, checkout.dest, "context.checkout.dest", required=False)
        if checkout.HasField("name"):
            if checkout.mode != template_pb.TEMPLATE_CHECKOUT_MODE_DIRECTORY:
                out.append(
                    "context.checkout.name: only valid in directory checkout mode"
                )
            name = checkout.name
            if name in (".", "..") or "/" in name or not 1 <= len(name) <= 255:
                out.append("context.checkout.name: invalid directory name")
        if len(ctx.ignore) > 256:
            out.append("context.ignore: at most 256 rules")
        for i, rule in enumerate(ctx.ignore):
            if len(rule) > 4096:
                out.append(f"context.ignore[{i}]: must be at most 4096 characters")

    if len(p.steps) > 256:
        out.append("steps: at most 256 steps")
    if any(step.WhichOneof("operation") == "copy" for step in p.steps) and not p.HasField(
        "context"
    ):
        out.append("steps: copy steps require a Git context")
    for i, step in enumerate(p.steps):
        field = f"steps[{i}]"
        if step.HasField("name") and not 1 <= len(step.name) <= 128:
            out.append(f"{field}.name: must be 1-128 characters")
        op = step.WhichOneof("operation")
        if op is None:
            out.append(f"{field}: exactly one step operation is required")
        elif op == "run":
            _check_command_or_argv(out, step.run, f"{field}.run")
            _check_path(out, step.run.workdir, f"{field}.run.workdir", required=False)
            if step.run.timeout_seconds > 86400:
                out.append(f"{field}.run.timeout_seconds: must be at most 86400")
        elif op == "copy":
            if not step.copy.src or len(step.copy.src) > 4096:
                out.append(f"{field}.copy.src: must be 1-4096 characters")
            elif not _context_relative(step.copy.src):
                out.append(
                    f"{field}.copy.src: must be context-relative and cannot"
                    " traverse above context"
                )
            _check_path(out, step.copy.dest, f"{field}.copy.dest")
        elif op == "write_file":
            _check_path(out, step.write_file.path, f"{field}.write_file.path")
            if len(step.write_file.content) > 1048576:
                out.append(f"{field}.write_file.content: must be at most 1 MiB")
            if step.write_file.HasField("mode") and step.write_file.mode > 0o7777:
                out.append(f"{field}.write_file.mode: must be at most 0o7777")
        elif op == "mkdir":
            _check_path(out, step.mkdir.path, f"{field}.mkdir.path")
            if step.mkdir.HasField("mode") and step.mkdir.mode > 0o7777:
                out.append(f"{field}.mkdir.mode: must be at most 0o7777")
        elif op == "remove":
            _check_path(out, step.remove.path, f"{field}.remove.path")
        elif op == "rename":
            _check_path(out, step.rename.src, f"{field}.rename.src")
            _check_path(out, step.rename.dest, f"{field}.rename.dest")
        elif op == "symlink":
            if not step.symlink.target or len(step.symlink.target) > 4096:
                out.append(f"{field}.symlink.target: must be 1-4096 characters")
            _check_path(out, step.symlink.path, f"{field}.symlink.path")
        else:  # apt / pip / npm / bun
            packages = getattr(step, op).packages
            if not 1 <= len(packages) <= 1024:
                out.append(f"{field}.{op}.packages: must have 1-1024 packages")
            if len(set(packages)) != len(packages):
                out.append(f"{field}.{op}.packages: must be unique")
            for j, pkg in enumerate(packages):
                if not pkg or not _PACKAGE_RE.match(pkg):
                    out.append(
                        f"{field}.{op}.packages[{j}]: invalid package name {pkg!r}"
                    )

    if p.HasField("runtime"):
        _validate_runtime(out, p.runtime)

    if p.HasField("resources"):
        res = p.resources
        if not 0 <= res.cpu_cores <= 16:
            out.append("resources.cpu_cores: must be between 0 and 16")
        if res.memory_mb != 0 and not 512 <= res.memory_mb <= 65536:
            out.append("resources.memory_mb: must be 0 or between 512 and 65536")
        if res.memory_mb % 2 != 0:
            out.append("resources.memory_mb: must be aligned to 2 MiB")
        if res.disk_size_gb != 0 and not 5 <= res.disk_size_gb <= 100:
            out.append("resources.disk_size_gb: must be 0 or between 5 and 100")

    if len(p.build.env) > 256:
        out.append("build.env: at most 256 entries")
    return out


def _validate_runtime(out: list[str], runtime) -> None:
    entrypoint = runtime.WhichOneof("entrypoint")
    if entrypoint is None:
        out.append("runtime: exactly one runtime entrypoint is required")
    elif entrypoint == "start":
        _check_command_or_argv(out, runtime.start, "runtime.start")
        _check_path(out, runtime.start.workdir, "runtime.start.workdir", required=False)
    else:
        pc = runtime.process_compose
        if not pc.config_path or len(pc.config_path) > 4096:
            out.append("runtime.process_compose.config_path: must be 1-4096 characters")
        elif not _context_relative(pc.config_path):
            out.append(
                "runtime.process_compose.config_path: must be workdir-relative"
                " and cannot traverse above it"
            )
        _check_path(
            out, pc.workdir, "runtime.process_compose.workdir", required=False
        )
        if len(pc.env_files) > 32:
            out.append("runtime.process_compose.env_files: at most 32 files")
        if len(set(pc.env_files)) != len(pc.env_files):
            out.append("runtime.process_compose.env_files: must be unique")
        for i, env_file in enumerate(pc.env_files):
            if not env_file or len(env_file) > 4096:
                out.append(
                    f"runtime.process_compose.env_files[{i}]: must be 1-4096 characters"
                )
            elif not _context_relative(env_file):
                out.append(
                    f"runtime.process_compose.env_files[{i}]: must be"
                    " workdir-relative and cannot traverse above it"
                )

    if runtime.run_at == template_pb.TEMPLATE_RUNTIME_RUN_AT_BUILD and not runtime.HasField(
        "ready_when"
    ):
        out.append("runtime.ready_when: build runtime requires ready_when")
    if (
        runtime.snapshot_mode != template_pb.TEMPLATE_SNAPSHOT_MODE_UNSPECIFIED
        and runtime.run_at != template_pb.TEMPLATE_RUNTIME_RUN_AT_BUILD
    ):
        out.append("runtime.snapshot_mode: only valid for build runtime")
    if runtime.stop_grace_seconds > 300:
        out.append("runtime.stop_grace_seconds: must be at most 300")
    if len(runtime.env) > 256:
        out.append("runtime.env: at most 256 entries")

    if runtime.HasField("ready_when"):
        ready = runtime.ready_when
        if not 1 <= ready.timeout_seconds <= 86400:
            out.append(
                "runtime.ready_when.timeout_seconds: must be between 1 and 86400"
            )
        if ready.poll_interval_seconds > 60:
            out.append(
                "runtime.ready_when.poll_interval_seconds: must be at most 60"
            )
        if len(ready.checks) > 64:
            out.append("runtime.ready_when.checks: at most 64 checks")
        for i, check in enumerate(ready.checks):
            field = f"runtime.ready_when.checks[{i}]"
            kind = check.WhichOneof("check")
            if kind is None:
                out.append(f"{field}: exactly one readiness check is required")
            elif kind == "port":
                if not 1 <= check.port <= 65535:
                    out.append(f"{field}.port: must be between 1 and 65535")
            elif kind == "http":
                url = check.http.url
                if not url or len(url) > 4096 or not _HTTP_READY_URL_RE.match(url):
                    out.append(f"{field}.http.url: must be a localhost http(s) URL")
                codes = check.http.success_status_codes
                if len(codes) > 100:
                    out.append(
                        f"{field}.http.success_status_codes: at most 100 codes"
                    )
                if len(set(codes)) != len(codes):
                    out.append(f"{field}.http.success_status_codes: must be unique")
                if any(not 100 <= code <= 599 for code in codes):
                    out.append(
                        f"{field}.http.success_status_codes: codes must be"
                        " between 100 and 599"
                    )
            else:
                _check_command_or_argv(out, check.exec, f"{field}.exec")
                if check.exec.timeout_seconds > 300:
                    out.append(f"{field}.exec.timeout_seconds: must be at most 300")
