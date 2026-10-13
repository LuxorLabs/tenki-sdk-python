# Security policy

## Reporting a vulnerability

**Do not report security vulnerabilities through public issues, discussions or pull requests.** A public report tells an attacker about the hole before a fix exists, and every other installation is what pays for the gap.

Use [GitHub's private vulnerability reporting](https://github.com/LuxorLabs/tenki-sdk-python/security/advisories/new) instead. It opens a private thread with the maintainers on this repository.

Tell us what you found, how to reproduce it, and what an attacker gets out of it.

There is no guaranteed response and no timeline; the SDK is published as it is. If we do act on a report, we will say so and coordinate the disclosure with you.

Send it privately regardless of whether we answer.

## What is in scope

This repository: the `tenki` / `tenki_sandbox` Python client, its transport and credential handling, and the generated protobuf bindings it ships.

Two things are out of scope because they are not ours to fix here, though we would still like to hear about them: the Tenki Cloud service the SDK talks to (report those through Tenki Cloud's own channel), and bugs in your own code that uses the SDK.

## What deserves a report

The SDK carries an auth token and session credentials and opens connections to the API and to per-session data-plane endpoints, so the interesting failures are about that boundary:

- A credential — your auth token or a session credential — reaching somewhere it should not: a log line, an exception message, a traceback, or a request to a host other than the one it was issued for.
- The client connecting without verifying TLS, or honoring a server-supplied endpoint that redirects credentials to an unintended host.
- A way to make the client execute, read, or write outside the sandbox session it was scoped to.

## Supported versions

There are no long-term support branches. Whatever gets fixed goes out in the next release of the `tenki` package on PyPI, and nothing is backported. Upgrade to the latest release before reporting.
