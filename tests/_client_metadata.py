import platform
from importlib.metadata import version

EXPECTED_CLIENT_METADATA = {
    "x-tenki-client-family": "python",
    "x-tenki-client-version": version("tenki"),
    "x-tenki-client-generation": "1",
    "x-tenki-client-runtime": f"{platform.python_implementation().lower()}/{platform.python_version()}",
}


def client_metadata_of(headers) -> dict:
    received = {key.lower(): value for key, value in dict(headers).items()}
    return {key: received.get(key) for key in EXPECTED_CLIENT_METADATA}
