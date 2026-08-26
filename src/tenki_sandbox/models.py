from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Mapping

from .errors import CommandFailedError

if TYPE_CHECKING:
    from .template_spec import TemplateSpec


@dataclass(frozen=True)
class CommandResult:
    argv: list[str]
    exit_code: int
    stdout: bytes = b""
    stderr: bytes = b""
    signal: str | None = None
    duration_ms: int | None = None
    reason: str | None = None
    errno: int | None = None
    #: Execution budget spent. May be set alongside ``exit_code == 0``.
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signal and not self.timed_out

    @property
    def stdout_text(self) -> str:
        return self.stdout.decode(errors="replace")

    @property
    def stderr_text(self) -> str:
        return self.stderr.decode(errors="replace")

    def check(self) -> "CommandResult":
        if not self.ok:
            raise CommandFailedError(self)
        return self


@dataclass(frozen=True)
class VolumeMount:
    volume_id: str
    mount_path: str
    read_only: bool = False
    state: str = ""


@dataclass(frozen=True)
class SandboxInfo:
    id: str
    name: str = ""
    state: str = "UNSPECIFIED"
    workspace_id: str = ""
    inbound_enabled: bool = False
    outbound_enabled: bool = False
    cpu_cores: int = 0
    memory_mb: int = 0
    disk_size_gb: int = 0
    timeout_at: datetime | None = None
    pause_expires_at: datetime | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    volume_mounts: tuple[VolumeMount, ...] = ()
    sticky: bool = False
    source_registry_image_id: str | None = None
    source_snapshot_id: str | None = None
    source_registry_workspace_id: str | None = None
    source_registry_ref: str | None = None
    source_template_id: str | None = None
    runtime_state: str = "UNSPECIFIED"
    runtime_error: str | None = None
    idle_timeout_minutes: int | None = None
    last_activity_at: datetime | None = None
    paused_at: datetime | None = None
    pause_snapshot_id: str | None = None
    pause_retention: timedelta | None = None
    last_resume_error: str = ""
    terminal_error: str = ""


@dataclass(frozen=True)
class ExposedPort:
    port: int
    url: str
    expires_at: datetime | None = None
    preview_url_id: str = ""
    slug: str = ""
    # Whether this exposure uses the workspace wildcard domain scheme.
    wildcard: bool = False
    # Readiness of the backing workspace wildcard certificate:
    # UNSPECIFIED / PENDING / READY / FAILED / DISABLED.
    wildcard_status: str = "UNSPECIFIED"
    # Actionable reason when wildcard_status is FAILED; empty otherwise.
    wildcard_status_reason: str = ""


@dataclass(frozen=True)
class PreviewUrl:
    id: str
    workspace_id: str
    owner_id: str
    slug: str
    token: str
    url: str
    session_id: str | None = None
    port: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_accessed_at: datetime | None = None
    # Whether this preview URL uses the workspace wildcard domain scheme.
    wildcard: bool = False
    # Readiness of the workspace wildcard domain backing this preview URL;
    # meaningful only when wildcard is True. One of UNSPECIFIED, PENDING,
    # READY, FAILED, DISABLED.
    wildcard_status: str = "UNSPECIFIED"
    # Actionable reason when wildcard_status is FAILED; empty otherwise.
    wildcard_status_reason: str = ""


@dataclass(frozen=True)
class FileInfo:
    path: str
    size: int
    mode: int
    is_dir: bool
    modified_unix_ns: int
    is_symlink: bool = False
    symlink_target: str = ""


@dataclass(frozen=True)
class VolumeAttachment:
    id: str
    volume_id: str
    session_id: str
    mount_path: str
    read_only: bool
    state: str


@dataclass(frozen=True)
class Volume:
    id: str
    workspace_id: str
    name: str
    size_bytes: int
    state: str
    tags: tuple[str, ...] = ()
    active_attachments: tuple["VolumeAttachment", ...] = ()

    def is_deletable(self) -> bool:
        return self.state == "AVAILABLE" and len(self.active_attachments) == 0


@dataclass(frozen=True)
class Snapshot:
    id: str
    session_id: str
    state: str
    name: str | None = None
    workspace_id: str | None = None
    size_bytes: int = 0
    compressed_bytes: int = 0
    memory_bytes: int = 0
    base_image_id: str = ""
    created_at: datetime | None = None
    expires_at: datetime | None = None
    tags: tuple[str, ...] = ()
    failure_reason: str = ""
    type: str = "UNSPECIFIED"
    durability_state: str = "UNSPECIFIED"
    cpu_cores: int = 0
    memory_mb: int = 0
    disk_size_gb: int = 0
    raw_image_available: bool = False


@dataclass(frozen=True)
class SnapshotDownloadURL:
    url: str
    expires_at: datetime | None = None


@dataclass(frozen=True)
class IdentityWorkspace:
    id: str
    name: str


@dataclass(frozen=True)
class Identity:
    owner_type: str
    owner_id: str
    workspaces: tuple["IdentityWorkspace", ...] = ()


@dataclass(frozen=True)
class TemplateResources:
    cpu_cores: int = 0
    memory_mb: int = 0
    disk_size_gb: int = 0


@dataclass(frozen=True)
class TemplateBuildStepReference:
    index: int
    kind: str
    label: str
    name: str | None = None


@dataclass(frozen=True)
class TemplateBuildLogEvent:
    type: Literal["log"]
    timestamp: datetime | None
    phase: str
    stream: Literal["stdout", "stderr", "system"]
    data: str
    step: TemplateBuildStepReference | None = None


@dataclass(frozen=True)
class TemplateBuildProgressEvent:
    type: Literal["progress"]
    timestamp: datetime | None
    phase: str
    state: Literal["unspecified", "started", "completed", "failed"]
    step: TemplateBuildStepReference | None = None
    message: str | None = None


TemplateBuildEvent = TemplateBuildLogEvent | TemplateBuildProgressEvent


@dataclass(frozen=True)
class TemplateBuildProvenance:
    requested_git_ref: str | None = None
    resolved_git_commit_sha: str | None = None
    build_secret_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemplateBuildFailure:
    code: str
    message: str
    step: TemplateBuildStepReference | None = None


@dataclass(frozen=True)
class Image:
    """Private registry image produced by a successful template build.

    ``ref`` is the untagged image ref (tracks the latest eligible build);
    ``digest_ref`` pins this exact build (``name@sha256:...``). This is the
    normal sandbox launch source: ``client.create(image=build.image)``.
    """

    ref: str
    digest: str | None = None
    digest_ref: str | None = None
    name: str = ""
    registry_image_id: str = ""
    workspace_id: str = ""


@dataclass(frozen=True)
class TemplateBuild:
    id: str
    template_id: str
    state: str
    version: int = 0
    error: str | None = None
    snapshot_id: str | None = None
    session_id: str | None = None
    spec: "TemplateSpec | None" = None
    spec_hash: str | None = None
    image: Image | None = None
    events: tuple[TemplateBuildEvent, ...] = ()
    provenance: TemplateBuildProvenance | None = None
    failure: TemplateBuildFailure | None = None


@dataclass(frozen=True)
class Template:
    id: str
    workspace_id: str
    name: str
    base_image_id: str = ""
    setup_script: str = ""
    start_cmd: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    resources: TemplateResources | None = None
    latest_build: TemplateBuild | None = None
    tags: tuple[str, ...] = ()
    visibility: str = "UNSPECIFIED"
    definition_mode: str = "UNSPECIFIED"
    parent_workspace_id: str = ""
    spec: "TemplateSpec | None" = None
    spec_hash: str | None = None


@dataclass(frozen=True)
class RegistryTag:
    id: str
    image_id: str
    tag: str
    snapshot_id: str
    ref: str
    updated_at: datetime | None = None


@dataclass(frozen=True)
class RegistryImage:
    id: str
    workspace_id: str
    workspace_slug: str
    name: str
    kind: str = "UNSPECIFIED"
    visibility: str = "UNSPECIFIED"
    title: str | None = None
    description: str | None = None
    labels: tuple[str, ...] = ()
    source_template_id: str | None = None
    source_snapshot_id: str | None = None
    tags: tuple["RegistryTag", ...] = ()
    created_at: datetime | None = None
    updated_at: datetime | None = None
    changes_not_yet_published: bool = False


@dataclass(frozen=True)
class RegistryImageSummary:
    id: str
    workspace_id: str
    workspace_slug: str
    name: str
    kind: str = "UNSPECIFIED"
    visibility: str = "UNSPECIFIED"
    labels: tuple[str, ...] = ()
    tags: tuple["RegistryTag", ...] = ()
    latest_snapshot_id: str | None = None
    latest_ref: str = ""
    updated_at: datetime | None = None
    changes_not_yet_published: bool = False


@dataclass(frozen=True)
class RegistryImageDetail:
    image: "RegistryImage"
    resolved_snapshot_id: str | None = None
    resolved_ref: str | None = None
    workspace_active: bool = False
    tombstoned: bool = False
    masked_env_var_keys: tuple[str, ...] = ()
    metadata: Mapping[str, str] = field(default_factory=dict)
    env_vars: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedRegistryRef:
    image_id: str
    owning_workspace_id: str
    owning_workspace_slug: str
    image_name: str
    tag: str | None = None
    snapshot_id: str | None = None
    kind: str = "UNSPECIFIED"
    visibility: str = "UNSPECIFIED"


@dataclass(frozen=True)
class RegistryShareGrant:
    id: str
    image_id: str
    owner_workspace_id: str
    target_workspace_id: str
    current_snapshot_id: str
    accepted_at: datetime | None = None
    revoked_at: datetime | None = None
    granted_via_token_id: str | None = None
    accepted_by: str | None = None
    follow_mode: str = ""
    follow_tag: str | None = None


@dataclass(frozen=True)
class RegistryShareResult:
    image: RegistryImage | None = None
    current_snapshot_id: str = ""
    digest_ref: str = ""
    grant: RegistryShareGrant | None = None


@dataclass(frozen=True)
class RegistryVersionDeleteResult:
    image_id: str
    snapshot_id: str


@dataclass(frozen=True)
class RegistryListResult:
    images: tuple["RegistryImageSummary", ...] = ()
    next_cursor: str = ""


@dataclass(frozen=True)
class SSHCert:
    ssh_cert: str
    ca_pub: str
    expires_at: datetime | None = None
    cert_serial: str = ""


@dataclass(frozen=True)
class TunnelTermination:
    reason: str
    message: str = ""


@dataclass(frozen=True)
class WorkspaceUsageLimit:
    """One usage dimension for a workspace (e.g. active sessions, snapshots).

    ``current`` counts resources holding live VMs (paused sessions excluded,
    matching how the concurrency quota is enforced on create); ``max`` is the
    effective limit, or ``None`` when unlimited / not set.
    """

    key: str
    label: str
    unit: str  # "count" | "bytes" | "unspecified"
    current: int
    max: int | None = None
    help_text: str = ""
