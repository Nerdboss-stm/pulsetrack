"""
PulseTrack secrets management.

Top-level facade for credential resolution. Sources are tried in priority:

    1. AWS Secrets Manager (production path)
    2. Process environment variables (CI, dev override)
    3. ``.env`` file via pydantic-settings (local dev)

Why ``pt_secrets`` and not ``secrets``?
    Python's stdlib has ``import secrets`` (cryptographic random). Naming
    this package ``secrets`` would shadow that and break
    ``data_generators/whoop_api/auth.py`` which uses
    ``secrets.token_urlsafe()`` for OAuth CSRF state.

Typical usage::

    from pt_secrets import get_secret_field
    whoop_client_id = get_secret_field("whoop", "client_id")
"""

from pt_secrets.manager import (
    SecretsError,
    SecretsManager,
    get_secret,
    get_secret_field,
    prefetch,
)

__all__ = [
    "SecretsError",
    "SecretsManager",
    "get_secret",
    "get_secret_field",
    "prefetch",
]
