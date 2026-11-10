import platform
from importlib.metadata import version

EXPECTED_CLIENT_METADATA = {
    "x-luxor-client-family": "python",
    "x-luxor-client-version": version("tenki"),
    "x-luxor-client-generation": "1",
    "x-luxor-client-runtime": f"{platform.python_implementation().lower()}/{platform.python_version()}",
}


def client_metadata_of(headers) -> dict:
    received = {key.lower(): value for key, value in dict(headers).items()}
    return {key: received.get(key) for key in EXPECTED_CLIENT_METADATA}
