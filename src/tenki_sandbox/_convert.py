from __future__ import annotations

from google.protobuf.json_format import MessageToDict

from ._pb import pb, registry_pb, template_pb
from ._time import datetime_from_proto
from .models import (
    ExposedPort,
    FileInfo,
    Identity,
    IdentityWorkspace,
    Image,
    PreviewUrl,
    RegistryImage,
    RegistryImageDetail,
    RegistryImageSummary,
    RegistryListResult,
    RegistryShareGrant,
    RegistryShareResult,
    RegistryTag,
    RegistryVersionDeleteResult,
    ResolvedRegistryRef,
    SessionCPUUsageAverage,
    SessionMemoryUsageAverage,
    SessionMetrics,
    SandboxInfo,
    SessionEgressPolicy,
    Snapshot,
    SnapshotDownloadURL,
    Template,
    TemplateBuild,
    TemplateBuildFailure,
    TemplateBuildLogEvent,
    TemplateBuildProgressEvent,
    TemplateBuildProvenance,
    TemplateBuildStepReference,
    TemplateResources,
    Volume,
    VolumeAttachment,
    VolumeMount,
    WorkspaceUsageLimit,
)
from .template_spec import TemplateSpec

_USAGE_UNITS = {1: "count", 2: "bytes"}  # SANDBOX_USAGE_UNIT_COUNT / _BYTES


def workspace_usage_limit(proto) -> WorkspaceUsageLimit:
    return WorkspaceUsageLimit(
        key=proto.key,
        label=proto.label,
        unit=_USAGE_UNITS.get(proto.unit, "unspecified"),
        current=int(proto.current),
        max=int(proto.max) if proto.HasField("max") else None,
        help_text=proto.help_text,
    )


def session_metrics(proto) -> SessionMetrics:
    return SessionMetrics(
        session_id=proto.session_id,
        requested_window=_duration_seconds(proto.requested_window) if proto.HasField("requested_window") else 0,
        window_start=datetime_from_proto(proto.window_start) if proto.HasField("window_start") else None,
        window_end=datetime_from_proto(proto.window_end) if proto.HasField("window_end") else None,
        cpu=_cpu_usage_average(proto.cpu) if proto.HasField("cpu") else SessionCPUUsageAverage(),
        memory=_memory_usage_average(proto.memory) if proto.HasField("memory") else SessionMemoryUsageAverage(),
    )


def _cpu_usage_average(proto) -> SessionCPUUsageAverage:
    return SessionCPUUsageAverage(
        average_cores=proto.average_cores if proto.HasField("average_cores") else None,
        average_percent=proto.average_percent if proto.HasField("average_percent") else None,
        limit_cores=proto.limit_cores,
        sample_count=int(proto.sample_count),
        observed_duration=_duration_seconds(proto.observed_duration) if proto.HasField("observed_duration") else 0,
        coverage_percent=proto.coverage_percent,
        first_sample_at=datetime_from_proto(proto.first_sample_at) if proto.HasField("first_sample_at") else None,
        last_sample_at=datetime_from_proto(proto.last_sample_at) if proto.HasField("last_sample_at") else None,
    )


def _memory_usage_average(proto) -> SessionMemoryUsageAverage:
    return SessionMemoryUsageAverage(
        average_bytes=proto.average_bytes if proto.HasField("average_bytes") else None,
        average_percent=proto.average_percent if proto.HasField("average_percent") else None,
        limit_bytes=int(proto.limit_bytes),
        sample_count=int(proto.sample_count),
        observed_duration=_duration_seconds(proto.observed_duration) if proto.HasField("observed_duration") else 0,
        coverage_percent=proto.coverage_percent,
        first_sample_at=datetime_from_proto(proto.first_sample_at) if proto.HasField("first_sample_at") else None,
        last_sample_at=datetime_from_proto(proto.last_sample_at) if proto.HasField("last_sample_at") else None,
    )


def _duration_seconds(value) -> float:
    return value.seconds + value.nanos / 1_000_000_000


def session_state(value: int) -> str:
    return pb.SessionState.Name(value).removeprefix("SESSION_STATE_")


def snapshot_state(value: int) -> str:
    return pb.SnapshotState.Name(value).removeprefix("SNAPSHOT_STATE_")


def volume_state(value: int) -> str:
    return pb.VolumeState.Name(value).removeprefix("VOLUME_STATE_")


def template_build_state(value: int) -> str:
    return template_pb.TemplateBuildState.Name(value).removeprefix(
        "TEMPLATE_BUILD_STATE_"
    )


def template_visibility(value: int) -> str:
    return template_pb.TemplateVisibility.Name(value).removeprefix(
        "TEMPLATE_VISIBILITY_"
    )


def template_definition_mode(value: int) -> str:
    return template_pb.TemplateDefinitionMode.Name(value).removeprefix(
        "TEMPLATE_DEFINITION_MODE_"
    )


def snapshot_type(value: int) -> str:
    return pb.SnapshotType.Name(value).removeprefix("SNAPSHOT_TYPE_")


def snapshot_durability_state(value: int) -> str:
    return pb.SnapshotDurabilityState.Name(value).removeprefix(
        "SNAPSHOT_DURABILITY_STATE_"
    )


def session_egress_policy(proto) -> SessionEgressPolicy:
    # An unset optional message field yields an empty default instance, never None.
    return SessionEgressPolicy(
        allow_domains=tuple(proto.allow_domains),
        allow_cidrs=tuple(proto.allow_cidrs),
    )


def sandbox_info(proto) -> SandboxInfo:
    return SandboxInfo(
        id=proto.id,
        name=proto.name,
        state=session_state(proto.state),
        workspace_id=proto.workspace_id,
        inbound_enabled=proto.inbound_enabled,
        outbound_enabled=proto.outbound_enabled,
        cpu_cores=proto.cpu_cores,
        memory_mb=proto.memory_mb,
        disk_size_gb=proto.disk_size_gb,
        timeout_at=datetime_from_proto(proto.timeout_at)
        if proto.HasField("timeout_at")
        else None,
        idle_timeout_minutes=(
            proto.idle_timeout_minutes
            if proto.HasField("idle_timeout_minutes")
            else None
        ),
        last_activity_at=datetime_from_proto(proto.last_activity_at)
        if proto.HasField("last_activity_at")
        else None,
        paused_at=datetime_from_proto(proto.paused_at)
        if proto.HasField("paused_at")
        else None,
        pause_snapshot_id=(
            proto.pause_snapshot_id if proto.HasField("pause_snapshot_id") else None
        ),
        pause_retention=(
            proto.pause_retention.ToTimedelta()
            if proto.HasField("pause_retention")
            else None
        ),
        pause_expires_at=datetime_from_proto(proto.pause_expires_at)
        if proto.HasField("pause_expires_at")
        else None,
        last_resume_error=proto.last_resume_error,
        terminal_error=proto.terminal_error,
        metadata=dict(proto.metadata),
        tags=tuple(proto.tags),
        egress=session_egress_policy(proto.egress),
        volume_mounts=tuple(
            VolumeMount(
                volume_id=mount.volume_id,
                mount_path=mount.mount_path,
                read_only=mount.readonly,
                state=mount.state,
            )
            for mount in proto.volume_attachments
        ),
        sticky=proto.sticky,
        source_registry_image_id=(
            proto.source_registry_image_id
            if proto.HasField("source_registry_image_id")
            else None
        ),
        source_snapshot_id=proto.source_snapshot_id
        if proto.HasField("source_snapshot_id")
        else None,
        source_registry_workspace_id=(
            proto.source_registry_workspace_id
            if proto.HasField("source_registry_workspace_id")
            else None
        ),
        source_registry_ref=proto.source_registry_ref
        if proto.HasField("source_registry_ref")
        else None,
        source_template_id=proto.source_template_id
        if proto.HasField("source_template_id")
        else None,
        has_runtime_secrets=proto.has_runtime_secrets,
        runtime_state=template_pb.TemplateRuntimeState.Name(proto.runtime_state).removeprefix(
            "TEMPLATE_RUNTIME_STATE_"
        ),
        runtime_error=proto.runtime_error if proto.HasField("runtime_error") else None,
    )


def exposed_port(resp) -> ExposedPort:
    return ExposedPort(
        port=resp.port,
        url=resp.preview_url,
        expires_at=datetime_from_proto(resp.expires_at)
        if resp.HasField("expires_at")
        else None,
        preview_url_id=resp.preview_url_id,
        slug=resp.slug,
        wildcard=resp.wildcard if resp.HasField("wildcard") else False,
        wildcard_status=_wildcard_status_name(resp),
        wildcard_status_reason=resp.wildcard_status_reason
        if resp.HasField("wildcard_status_reason")
        else "",
    )


def _wildcard_status_name(proto) -> str:
    if not proto.HasField("wildcard_status"):
        return "UNSPECIFIED"
    try:
        name = pb.WildcardStatus.Name(proto.wildcard_status)
    except ValueError:
        # Proto3 enums are open: a newer server can send a value this SDK does
        # not know, which protobuf preserves numerically but Name() rejects.
        # An older client must degrade to UNSPECIFIED, not crash.
        return "UNSPECIFIED"
    return name.removeprefix("WILDCARD_STATUS_")


def preview_url(proto) -> PreviewUrl:
    return PreviewUrl(
        id=proto.id,
        workspace_id=proto.workspace_id,
        owner_id=proto.owner_id,
        slug=proto.slug,
        token=proto.token,
        url=proto.preview_url,
        session_id=proto.session_id if proto.HasField("session_id") else None,
        port=proto.port if proto.HasField("port") else None,
        created_at=datetime_from_proto(proto.created_at)
        if proto.HasField("created_at")
        else None,
        updated_at=datetime_from_proto(proto.updated_at)
        if proto.HasField("updated_at")
        else None,
        last_accessed_at=datetime_from_proto(proto.last_accessed_at)
        if proto.HasField("last_accessed_at")
        else None,
        wildcard=proto.wildcard if proto.HasField("wildcard") else False,
        wildcard_status=_wildcard_status_name(proto),
        wildcard_status_reason=proto.wildcard_status_reason
        if proto.HasField("wildcard_status_reason")
        else "",
        expires_at=datetime_from_proto(proto.expires_at)
        if proto.HasField("expires_at")
        else None,
    )


def file_info(path: str, proto) -> FileInfo:
    return FileInfo(
        path=path,
        size=int(proto.size),
        mode=proto.mode,
        is_dir=proto.is_dir,
        modified_unix_ns=int(proto.modified_unix_ns),
        is_symlink=getattr(proto, "is_symlink", False),
        symlink_target=getattr(proto, "symlink_target", ""),
    )


def volume_attachment(proto) -> VolumeAttachment:
    return VolumeAttachment(
        id=proto.id,
        volume_id=proto.volume_id,
        session_id=proto.session_id,
        mount_path=proto.mount_path,
        read_only=proto.readonly,
        state=proto.state,
    )


def volume(proto, active_attachments=()) -> Volume:
    return Volume(
        id=proto.id,
        workspace_id=proto.workspace_id,
        name=proto.name,
        size_bytes=int(proto.size_bytes),
        state=volume_state(proto.state),
        tags=tuple(proto.tags),
        active_attachments=tuple(volume_attachment(a) for a in active_attachments),
    )


def snapshot(proto) -> Snapshot:
    return Snapshot(
        id=proto.id,
        session_id=proto.session_id,
        state=snapshot_state(proto.state),
        name=proto.name if proto.HasField("name") else None,
        workspace_id=proto.workspace_id if proto.HasField("workspace_id") else None,
        size_bytes=int(proto.size_bytes),
        compressed_bytes=int(proto.compressed_bytes),
        memory_bytes=int(proto.memory_bytes),
        base_image_id=proto.base_image_id,
        created_at=datetime_from_proto(proto.created_at)
        if proto.HasField("created_at")
        else None,
        expires_at=datetime_from_proto(proto.expires_at)
        if proto.HasField("expires_at")
        else None,
        tags=tuple(proto.tags),
        failure_reason=proto.failure_reason,
        type=snapshot_type(proto.type),
        durability_state=snapshot_durability_state(proto.durability_state),
        cpu_cores=proto.cpu_cores,
        memory_mb=proto.memory_mb,
        disk_size_gb=proto.disk_size_gb,
        raw_image_available=proto.raw_image_available,
        has_runtime_secrets=proto.has_runtime_secrets,
    )


def snapshot_download_url(proto) -> SnapshotDownloadURL:
    return SnapshotDownloadURL(
        url=proto.url,
        expires_at=datetime_from_proto(proto.expires_at)
        if proto.HasField("expires_at")
        else None,
    )


def template_resources(proto) -> TemplateResources | None:
    if proto is None:
        return None
    return TemplateResources(
        cpu_cores=proto.cpu_cores,
        memory_mb=proto.memory_mb,
        disk_size_gb=proto.disk_size_gb,
    )


def _build_image(proto) -> Image | None:
    if not proto.HasField("image"):
        return None
    name = proto.image.name
    slug = proto.image.workspace_slug
    return Image(
        ref=f"{slug}/{name}" if slug and name else name,
        digest=proto.image_digest if proto.HasField("image_digest") else None,
        digest_ref=proto.image_digest_ref
        if proto.HasField("image_digest_ref")
        else None,
        name=name,
        registry_image_id=proto.image.id,
        workspace_id=proto.image.workspace_id,
    )


def template_build(proto) -> TemplateBuild | None:
    if proto is None:
        return None
    return TemplateBuild(
        id=proto.id,
        template_id=proto.template_id,
        state=template_build_state(proto.state),
        version=proto.version,
        error=proto.error if proto.HasField("error") else None,
        snapshot_id=proto.snapshot_id if proto.HasField("snapshot_id") else None,
        session_id=proto.session_id if proto.HasField("session_id") else None,
        spec=TemplateSpec.from_proto(proto.builder_spec)
        if proto.HasField("builder_spec")
        else None,
        spec_hash=proto.spec_hash if proto.HasField("spec_hash") else None,
        image=_build_image(proto),
        events=tuple(
            converted
            for event in proto.events
            if (converted := _template_build_event(event)) is not None
        ),
        provenance=TemplateBuildProvenance(
            requested_git_ref=proto.provenance.requested_git_ref
            if proto.provenance.HasField("requested_git_ref")
            else None,
            resolved_git_commit_sha=proto.provenance.resolved_git_commit_sha
            if proto.provenance.HasField("resolved_git_commit_sha")
            else None,
            build_secret_keys=tuple(proto.provenance.build_secret_keys),
        )
        if proto.HasField("provenance")
        else None,
        failure=TemplateBuildFailure(
            code=proto.failure.code,
            message=proto.failure.message,
            step=_template_build_step(proto.failure.step)
            if proto.failure.HasField("step")
            else None,
        )
        if proto.HasField("failure")
        else None,
    )


def _template_build_step(proto) -> TemplateBuildStepReference:
    return TemplateBuildStepReference(
        index=proto.index,
        kind=proto.kind,
        label=proto.label,
        name=proto.name if proto.HasField("name") else None,
    )


def _template_build_event(proto):
    kind = proto.WhichOneof("event")
    if kind == "log":
        value = proto.log
        stream = (
            template_pb.TemplateBuildLogStream.Name(value.stream)
            .removeprefix("TEMPLATE_BUILD_LOG_STREAM_")
            .lower()
        )
        return TemplateBuildLogEvent(
            type="log",
            timestamp=datetime_from_proto(value.timestamp)
            if value.HasField("timestamp")
            else None,
            phase=value.phase,
            step=_template_build_step(value.step) if value.HasField("step") else None,
            stream=stream if stream in ("stdout", "stderr") else "system",
            data=value.data,
        )
    if kind != "progress":
        return None
    value = proto.progress
    state = (
        template_pb.TemplateBuildProgressState.Name(value.state)
        .removeprefix("TEMPLATE_BUILD_PROGRESS_STATE_")
        .lower()
    )
    return TemplateBuildProgressEvent(
        type="progress",
        timestamp=datetime_from_proto(value.timestamp)
        if value.HasField("timestamp")
        else None,
        phase=value.phase,
        step=_template_build_step(value.step) if value.HasField("step") else None,
        state=state if state in ("started", "completed", "failed") else "unspecified",
        message=value.message if value.HasField("message") else None,
    )


def identity(proto) -> Identity:
    workspaces = [
        IdentityWorkspace(id=ws.workspace_id, name=ws.name)
        for ws in proto.workspaces
    ]
    return Identity(
        owner_type=proto.owner_type,
        owner_id=proto.owner_id,
        workspaces=tuple(workspaces),
    )


def registry_image_kind(value: int) -> str:
    return registry_pb.RegistryImageKind.Name(value).removeprefix(
        "REGISTRY_IMAGE_KIND_"
    )


def registry_visibility(value: int) -> str:
    return registry_pb.RegistryVisibility.Name(value).removeprefix(
        "REGISTRY_VISIBILITY_"
    )


def registry_tag(proto) -> RegistryTag:
    return RegistryTag(
        id=proto.id,
        image_id=proto.image_id,
        tag=proto.tag,
        snapshot_id=proto.snapshot_id,
        ref=proto.ref,
        updated_at=datetime_from_proto(proto.updated_at)
        if proto.HasField("updated_at")
        else None,
    )


def registry_image(proto) -> RegistryImage:
    return RegistryImage(
        id=proto.id,
        workspace_id=proto.workspace_id,
        workspace_slug=proto.workspace_slug,
        name=proto.name,
        kind=registry_image_kind(proto.kind),
        visibility=registry_visibility(proto.visibility),
        title=proto.title if proto.HasField("title") else None,
        description=proto.description if proto.HasField("description") else None,
        labels=tuple(proto.labels),
        source_template_id=proto.source_template_id
        if proto.HasField("source_template_id")
        else None,
        source_snapshot_id=proto.source_snapshot_id
        if proto.HasField("source_snapshot_id")
        else None,
        tags=tuple(registry_tag(t) for t in proto.tags),
        created_at=datetime_from_proto(proto.created_at)
        if proto.HasField("created_at")
        else None,
        updated_at=datetime_from_proto(proto.updated_at)
        if proto.HasField("updated_at")
        else None,
        changes_not_yet_published=proto.changes_not_yet_published,
    )


def registry_image_summary(proto) -> RegistryImageSummary:
    return RegistryImageSummary(
        id=proto.id,
        workspace_id=proto.workspace_id,
        workspace_slug=proto.workspace_slug,
        name=proto.name,
        kind=registry_image_kind(proto.kind),
        visibility=registry_visibility(proto.visibility),
        labels=tuple(proto.labels),
        tags=tuple(registry_tag(t) for t in proto.tags),
        latest_snapshot_id=proto.latest_snapshot_id
        if proto.HasField("latest_snapshot_id")
        else None,
        latest_ref=proto.latest_ref,
        updated_at=datetime_from_proto(proto.updated_at)
        if proto.HasField("updated_at")
        else None,
        changes_not_yet_published=proto.changes_not_yet_published,
    )


def registry_image_detail(proto) -> RegistryImageDetail:
    return RegistryImageDetail(
        image=registry_image(proto.image),
        resolved_snapshot_id=proto.resolved_snapshot_id
        if proto.HasField("resolved_snapshot_id")
        else None,
        resolved_ref=proto.resolved_ref if proto.HasField("resolved_ref") else None,
        workspace_active=proto.workspace_active,
        tombstoned=proto.tombstoned,
        masked_env_var_keys=tuple(proto.masked_env_var_keys),
        metadata=dict(proto.metadata),
        env_vars=dict(proto.env_vars),
        runtime_secret_env=dict(proto.runtime_secret_env),
        runtime_secret_files=tuple(MessageToDict(file) for file in proto.runtime_secret_files),
    )


def resolved_registry_ref(proto) -> ResolvedRegistryRef:
    return ResolvedRegistryRef(
        image_id=proto.image_id,
        owning_workspace_id=proto.owning_workspace_id,
        owning_workspace_slug=proto.owning_workspace_slug,
        image_name=proto.image_name,
        tag=proto.tag if proto.HasField("tag") else None,
        snapshot_id=proto.snapshot_id if proto.HasField("snapshot_id") else None,
        kind=registry_image_kind(proto.kind),
        visibility=registry_visibility(proto.visibility),
    )


def registry_share_grant(proto) -> RegistryShareGrant:
    return RegistryShareGrant(
        id=proto.id,
        image_id=proto.image_id,
        owner_workspace_id=proto.owner_workspace_id,
        target_workspace_id=proto.target_workspace_id,
        current_snapshot_id=proto.current_snapshot_id,
        accepted_at=datetime_from_proto(proto.accepted_at)
        if proto.HasField("accepted_at")
        else None,
        revoked_at=datetime_from_proto(proto.revoked_at)
        if proto.HasField("revoked_at")
        else None,
        granted_via_token_id=proto.granted_via_token_id
        if proto.HasField("granted_via_token_id")
        else None,
        accepted_by=proto.accepted_by if proto.HasField("accepted_by") else None,
        follow_mode=proto.follow_mode,
        follow_tag=proto.follow_tag if proto.HasField("follow_tag") else None,
    )


def registry_share_result(proto) -> RegistryShareResult:
    return RegistryShareResult(
        image=registry_image(proto.image) if proto.HasField("image") else None,
        current_snapshot_id=proto.current_snapshot_id,
        digest_ref=proto.digest_ref,
        grant=registry_share_grant(proto.grant) if proto.HasField("grant") else None,
    )


def registry_version_delete_result(proto) -> RegistryVersionDeleteResult:
    return RegistryVersionDeleteResult(
        image_id=proto.image_id,
        snapshot_id=proto.snapshot_id,
    )


def registry_list_result(proto) -> RegistryListResult:
    return RegistryListResult(
        images=tuple(registry_image_summary(image) for image in proto.images),
        next_cursor=proto.next_cursor,
    )


def template(proto) -> Template:
    return Template(
        id=proto.id,
        workspace_id=proto.workspace_id,
        name=proto.name,
        base_image_id=proto.base_image_id,
        setup_script=proto.setup_script,
        start_cmd=proto.start_cmd if proto.HasField("start_cmd") else None,
        env=dict(proto.env_vars),
        resources=template_resources(proto.resources)
        if proto.HasField("resources")
        else None,
        latest_build=template_build(proto.latest_build)
        if proto.HasField("latest_build")
        else None,
        tags=tuple(proto.tags),
        visibility=template_visibility(proto.visibility),
        definition_mode=template_definition_mode(proto.definition_mode),
        parent_workspace_id=proto.parent_workspace_id
        if proto.HasField("parent_workspace_id")
        else "",
        spec=TemplateSpec.from_proto(proto.builder_spec)
        if proto.HasField("builder_spec")
        else None,
        spec_hash=proto.spec_hash if proto.HasField("spec_hash") else None,
    )
