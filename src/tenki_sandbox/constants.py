DEFAULT_BASE_URL = "https://api.luxor.ai"
DEFAULT_CREATE_TIMEOUT = 180.0
DEFAULT_EXEC_TIMEOUT = 30.0
DEFAULT_SNAPSHOT_TIMEOUT = 300.0
# Snapshot waits retry "not found" this long: a lagging read replica may not have a just-created snapshot yet.
SNAPSHOT_NOT_FOUND_GRACE = 10.0
DEFAULT_TEMPLATE_BUILD_TIMEOUT = 600.0
DEFAULT_VOLUME_DETACH_TIMEOUT = 120.0

ENV_LUXOR_AUTH_TOKEN = "LUXOR_AUTH_TOKEN"
ENV_LUXOR_API_KEY = "LUXOR_API_KEY"
ENV_LUXOR_API_ENDPOINT = "LUXOR_API_ENDPOINT"
ENV_LUXOR_API_URL = "LUXOR_API_URL"
ENV_LUXOR_GATEWAY_URL = "LUXOR_SANDBOX_GATEWAY_URL"
ENV_LUXOR_CLOUD_API_URL = "LUXOR_CLOUD_API_URL"

ENV_AUTH_TOKEN = "TENKI_AUTH_TOKEN"
ENV_API_KEY = "TENKI_API_KEY"
ENV_API_ENDPOINT = "TENKI_API_ENDPOINT"
ENV_API_URL = "TENKI_API_URL"
ENV_GATEWAY_URL = "TENKI_SANDBOX_GATEWAY_URL"
ENV_CLOUD_API_URL = "TENKI_CLOUD_API_URL"

# Precedence order: every LUXOR_* name before its TENKI_* fallback.
AUTH_TOKEN_ENVS = (ENV_LUXOR_AUTH_TOKEN, ENV_LUXOR_API_KEY, ENV_AUTH_TOKEN, ENV_API_KEY)
BASE_URL_ENVS = (ENV_LUXOR_API_ENDPOINT, ENV_LUXOR_API_URL, ENV_API_ENDPOINT, ENV_API_URL)
GATEWAY_URL_ENVS = (ENV_LUXOR_GATEWAY_URL, ENV_GATEWAY_URL)
CLOUD_API_URL_ENVS = (ENV_LUXOR_CLOUD_API_URL, ENV_CLOUD_API_URL)

API_KEY_PREFIXES = ("lx_", "tk_")


def _sdk_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    for distribution in ("tenki", "luxor-ai"):
        try:
            return version(distribution)
        except PackageNotFoundError:
            continue
    return "unknown"


SDK_VERSION = _sdk_version()
SDK_USER_AGENT = f"luxor-sandbox-python/{SDK_VERSION}"

KB = 1_000
MB = 1_000_000
GB = 1_000_000_000
TB = 1_000_000_000_000

KiB = 1024
MiB = 1024**2
GiB = 1024**3
TiB = 1024**4


def snapshot_ready_poll_delay(elapsed: float) -> float:
    """Seconds before the next snapshot readiness poll; most snapshots are ready within a few seconds."""
    if elapsed < 3:
        return 0.1
    if elapsed < 15:
        return 0.25
    return 1.0
