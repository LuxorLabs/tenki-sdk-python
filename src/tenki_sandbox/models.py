from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping

from .errors import CommandFailedError


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

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signal

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
    project_id: str = ""
    workspace_id: str = ""
    cpu_cores: int = 0
    memory_mb: int = 0
    disk_size_gb: int = 0
    timeout_at: datetime | None = None
    pause_expires_at: datetime | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    volume_mounts: tuple[VolumeMount, ...] = ()
    sticky: bool = False
    source_registry_artifact_id: str | None = None
    source_snapshot_id: str | None = None
    source_registry_workspace_id: str | None = None
    source_registry_ref: str | None = None


@dataclass(frozen=True)
class ExposedPort:
    port: int
    url: str
    expires_at: datetime | None = None
    preview_url_id: str = ""
    slug: str = ""


@dataclass(frozen=True)
class PreviewUrl:
    id: str
    project_id: str
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
    project_id: str = ""
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
    project_id: str = ""
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
class IdentityProject:
    id: str
    name: str


@dataclass(frozen=True)
class IdentityWorkspace:
    id: str
    name: str
    projects: tuple["IdentityProject", ...] = ()


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
class TemplateBuild:
    id: str
    template_id: str
    state: str
    version: int = 0
    error: str | None = None
    snapshot_id: str | None = None
    session_id: str | None = None


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
    project_id: str = ""
    tags: tuple[str, ...] = ()
    visibility: str = "UNSPECIFIED"
    parent_workspace_id: str = ""


@dataclass(frozen=True)
class RegistryTag:
    id: str
    artifact_id: str
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
    artifact: "RegistryImage"
    resolved_snapshot_id: str | None = None
    resolved_ref: str | None = None
    workspace_active: bool = False
    tombstoned: bool = False
    masked_env_var_keys: tuple[str, ...] = ()
    metadata: Mapping[str, str] = field(default_factory=dict)
    env_vars: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedRegistryRef:
    artifact_id: str
    owning_workspace_id: str
    owning_workspace_slug: str
    artifact_name: str
    tag: str | None = None
    snapshot_id: str | None = None
    kind: str = "UNSPECIFIED"
    visibility: str = "UNSPECIFIED"


@dataclass(frozen=True)
class RegistryShareGrant:
    id: str
    artifact_id: str
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
class RegistryListResult:
    artifacts: tuple["RegistryImageSummary", ...] = ()
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
