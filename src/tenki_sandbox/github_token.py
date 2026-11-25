from __future__ import annotations

import base64

from .injection_credentials import InjectionCredentialInput, InjectionRule


def github_injection_credentials(token: str) -> list[InjectionCredentialInput]:
    token = token.strip()
    if not token:
        raise ValueError("GitHub token injection requires a nonempty github token")
    return [
        InjectionCredentialInput("SDK_GITHUB_API", token.encode(), [InjectionRule(
            "https://api.github.com", ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"], "/", "Authorization",
        )]),
        InjectionCredentialInput("SDK_GITHUB_GIT", base64.b64encode(("x-access-token:" + token).encode()), [InjectionRule(
            "https://github.com", ["GET", "POST"], "/", "Authorization",
        )]),
    ]


def github_injection_env(env) -> None:
    if env.get("GH_HOST") and env["GH_HOST"] != "github.com":
        raise ValueError("GitHub token injection supports github.com only")
    if any(key.startswith("GIT_CONFIG") for key in env):
        raise ValueError("GitHub token injection reserves GIT_CONFIG environment variables")
    env["GH_TOKEN"] = "secrets://SDK_GITHUB_API"
    env["GIT_TOKEN"] = env["GH_TOKEN"]
    settings = [
        ("http.https://github.com/.extraHeader", "Authorization: Basic secrets://SDK_GITHUB_GIT"),
        ("http.https://github.com/.version", "HTTP/1.1"),
        ("credential.https://github.com.helper", ""),
        ("http.https://github.com/.followRedirects", "false"),
    ]
    env["GIT_CONFIG_COUNT"] = str(len(settings))
    for i, (key, value) in enumerate(settings):
        env[f"GIT_CONFIG_KEY_{i}"] = key
        env[f"GIT_CONFIG_VALUE_{i}"] = value
