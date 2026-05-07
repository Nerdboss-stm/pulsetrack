"""
WHOOP OAuth 2.0 — Authorization Code flow with localhost callback.

First run: opens the user's browser to WHOOP's authorize URL, receives the
auth code on a local HTTP server, exchanges it for access + refresh tokens,
persists them to ``settings.whoop_token_path``.

Subsequent runs: load tokens from disk. If access token is expired (or about
to be), refresh using the refresh token. New tokens overwrite the file.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from utils.retry import retry  # noqa: E402

log = get_logger(__name__)

# Refresh tokens this many seconds before they actually expire.
REFRESH_LEEWAY_SECONDS = 300


class WhoopAuthError(RuntimeError):
    pass


def _token_file_path() -> str:
    return os.path.expanduser(settings.whoop_token_path)


def _read_tokens() -> Optional[dict]:
    path = _token_file_path()
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _write_tokens(tokens: dict) -> None:
    path = _token_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Compute absolute expiry once so callers can check is_expired() cheaply.
    if "expires_in" in tokens and "expires_at" not in tokens:
        tokens["expires_at"] = int(time.time()) + int(tokens["expires_in"])
    with open(path, "w") as f:
        json.dump(tokens, f)
    os.chmod(path, 0o600)  # owner read/write only — these are credentials


def _is_expired(tokens: dict) -> bool:
    expires_at = tokens.get("expires_at", 0)
    return time.time() >= (expires_at - REFRESH_LEEWAY_SECONDS)


def _build_authorize_url(state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": settings.whoop_client_id,
        "redirect_uri": settings.whoop_redirect_uri,
        "scope": settings.whoop_oauth_scopes,
        "state": state,
    }
    return f"{settings.whoop_oauth_authorize_url}?{urllib.parse.urlencode(params)}"


class _CallbackHandler(BaseHTTPRequestHandler):
    received_code: Optional[str] = None
    received_state: Optional[str] = None
    received_error: Optional[str] = None

    def do_GET(self):  # noqa: N802 — http.server callback name
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != urllib.parse.urlparse(settings.whoop_redirect_uri).path:
            self.send_response(404)
            self.end_headers()
            return
        params = urllib.parse.parse_qs(parsed.query)
        _CallbackHandler.received_code = params.get("code", [None])[0]
        _CallbackHandler.received_state = params.get("state", [None])[0]
        _CallbackHandler.received_error = params.get("error", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body><h1>WHOOP authorization complete.</h1>")
        self.wfile.write(b"<p>You can close this window.</p></body></html>")

    def log_message(self, *args, **kwargs):  # silence default stderr logs
        pass


def _run_callback_server(port: int, timeout_seconds: int = 300) -> dict:
    """Block until the OAuth callback fires (or timeout). Returns parsed callback params."""
    server = HTTPServer(("localhost", port), _CallbackHandler)
    server.timeout = 1
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + timeout_seconds
    try:
        while _CallbackHandler.received_code is None and _CallbackHandler.received_error is None:
            if time.time() > deadline:
                raise WhoopAuthError("OAuth callback timed out — no response within 5 min")
            time.sleep(0.5)
    finally:
        server.shutdown()
        thread.join(timeout=2)
    return {
        "code": _CallbackHandler.received_code,
        "state": _CallbackHandler.received_state,
        "error": _CallbackHandler.received_error,
    }


@retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
def _exchange_code_for_tokens(code: str) -> dict:
    response = requests.post(
        settings.whoop_oauth_token_url,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": settings.whoop_redirect_uri,
            "client_id": settings.whoop_client_id,
            "client_secret": settings.whoop_client_secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


@retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
def _refresh_tokens(refresh_token: str) -> dict:
    response = requests.post(
        settings.whoop_oauth_token_url,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": settings.whoop_client_id,
            "client_secret": settings.whoop_client_secret,
            "scope": settings.whoop_oauth_scopes,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def authorize_interactive() -> dict:
    """Run the browser-based OAuth flow. Returns persisted token dict."""
    if not settings.whoop_client_id or not settings.whoop_client_secret:
        raise WhoopAuthError(
            "WHOOP client credentials missing — set PT_WHOOP_CLIENT_ID and PT_WHOOP_CLIENT_SECRET"
        )

    redirect = urllib.parse.urlparse(settings.whoop_redirect_uri)
    if redirect.scheme != "http" or redirect.hostname != "localhost":
        raise WhoopAuthError(
            "whoop_redirect_uri must be a http://localhost URL for the local callback server"
        )

    state = secrets.token_urlsafe(32)
    authorize_url = _build_authorize_url(state)
    log.info(
        "Opening browser for WHOOP authorization",
        extra={"extra_data": {"redirect_uri": settings.whoop_redirect_uri}},
    )
    webbrowser.open(authorize_url)

    callback = _run_callback_server(redirect.port or 80)
    if callback["error"]:
        raise WhoopAuthError(f"WHOOP returned error: {callback['error']}")
    if callback["state"] != state:
        raise WhoopAuthError("State mismatch — possible CSRF attack")
    if not callback["code"]:
        raise WhoopAuthError("No authorization code returned")

    tokens = _exchange_code_for_tokens(callback["code"])
    _write_tokens(tokens)
    log.info("WHOOP tokens persisted", extra={"extra_data": {"path": _token_file_path()}})
    return tokens


def get_access_token() -> str:
    """Return a valid access token. Refreshes if expired; runs interactive auth if no tokens."""
    tokens = _read_tokens()
    if tokens is None:
        log.info("No persisted WHOOP tokens — running interactive authorization")
        tokens = authorize_interactive()

    if _is_expired(tokens):
        if "refresh_token" not in tokens:
            log.warning("Tokens expired and no refresh_token — re-authorizing")
            tokens = authorize_interactive()
        else:
            log.info("Refreshing WHOOP access token")
            new_tokens = _refresh_tokens(tokens["refresh_token"])
            # WHOOP returns a new refresh_token on each refresh; preserve if missing.
            if "refresh_token" not in new_tokens:
                new_tokens["refresh_token"] = tokens["refresh_token"]
            _write_tokens(new_tokens)
            tokens = new_tokens

    return tokens["access_token"]
