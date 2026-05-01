from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

import requests

from src.domain.tasks import text_or_blank

_logger = logging.getLogger(__name__)
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
SUPPORTED_GRAPH_AUTH_MODES = {
    "client_secret",
    "default_azure_credential",
    "managed_identity",
}


@dataclass(frozen=True, slots=True)
class GraphAuthSettings:
    auth_mode: str
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    base_url: str = "https://graph.microsoft.com/v1.0"
    timeout_seconds: int = 30

    def __repr__(self) -> str:
        return (
            f"GraphAuthSettings(auth_mode={self.auth_mode!r}, "
            f"tenant_id={self.tenant_id!r}, "
            f"client_id={self.client_id!r}, "
            f"client_secret='[REDACTED]', "
            f"base_url={self.base_url!r}, "
            f"timeout_seconds={self.timeout_seconds!r})"
        )


@dataclass(frozen=True, slots=True)
class SiteReference:
    hostname: str
    path: str


@dataclass(frozen=True, slots=True)
class GraphDownloadedFile:
    name: str
    content: bytes
    content_type: str
    site_id: str
    drive_id: str
    item_id: str
    web_url: str


def normalize_graph_auth_mode(value: str) -> str:
    mode = text_or_blank(value).lower()
    if not mode:
        return ""
    if mode not in SUPPORTED_GRAPH_AUTH_MODES:
        supported = ", ".join(sorted(SUPPORTED_GRAPH_AUTH_MODES))
        raise ValueError(f"Unsupported GRAPH_AUTH_MODE: {value}. Expected one of: {supported}")
    return mode


def load_graph_auth_settings() -> GraphAuthSettings:
    configured_mode = normalize_graph_auth_mode(os.getenv("GRAPH_AUTH_MODE", ""))
    has_client_secret = all(
        os.getenv(name, "").strip()
        for name in ("GRAPH_TENANT_ID", "GRAPH_CLIENT_ID", "GRAPH_CLIENT_SECRET")
    )
    auth_mode = configured_mode or ("client_secret" if has_client_secret else "default_azure_credential")

    return GraphAuthSettings(
        auth_mode=auth_mode,
        tenant_id=os.getenv("GRAPH_TENANT_ID", "").strip(),
        client_id=os.getenv("GRAPH_CLIENT_ID", "").strip(),
        client_secret=os.getenv("GRAPH_CLIENT_SECRET", "").strip(),
        base_url=os.getenv("GRAPH_BASE_URL", "https://graph.microsoft.com/v1.0").rstrip("/"),
        timeout_seconds=max(5, int(os.getenv("GRAPH_TIMEOUT_SECONDS", "30"))),
    )


def parse_site_url(site_url: str) -> SiteReference:
    parsed = urlparse(text_or_blank(site_url))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid Graph site_url: {site_url}")
    if not parsed.path or parsed.path == "/":
        raise ValueError("Graph site_url must point to a SharePoint site, not just the tenant root.")

    return SiteReference(
        hostname=parsed.netloc,
        path=parsed.path.rstrip("/"),
    )


def normalize_item_path(file_path: str) -> str:
    path = text_or_blank(file_path)
    if not path:
        raise ValueError("Graph file_path cannot be empty.")
    return "/" + path.lstrip("/")


def _retry_delay(response: requests.Response, attempt: int) -> float:
    try:
        return float(response.headers.get("Retry-After", ""))
    except (ValueError, TypeError):
        return _RETRY_BACKOFF_BASE * (2 ** attempt)


def graph_error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        return f"Graph request failed with {response.status_code}: {response.text}"

    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict):
        message = text_or_blank(error.get("message"))
        code = text_or_blank(error.get("code"))
        if code and message:
            return f"Graph request failed with {response.status_code}: {code} - {message}"
        if message:
            return f"Graph request failed with {response.status_code}: {message}"

    return f"Graph request failed with {response.status_code}: {payload}"


def build_token_credential(settings: GraphAuthSettings):
    if settings.auth_mode == "client_secret":
        if not settings.tenant_id or not settings.client_id or not settings.client_secret:
            raise RuntimeError(
                "GRAPH_TENANT_ID, GRAPH_CLIENT_ID, and GRAPH_CLIENT_SECRET are required for client_secret mode."
            )
        from azure.identity import ClientSecretCredential

        return ClientSecretCredential(
            tenant_id=settings.tenant_id,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
        )

    if settings.auth_mode == "managed_identity":
        from azure.identity import ManagedIdentityCredential

        return ManagedIdentityCredential(client_id=settings.client_id or None)

    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(
        exclude_interactive_browser_credential=False,
        managed_identity_client_id=settings.client_id or None,
    )


class GraphFileClient:
    def __init__(self, settings: GraphAuthSettings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.credential = build_token_credential(settings)

    @classmethod
    def from_env(cls) -> GraphFileClient:
        return cls(load_graph_auth_settings())

    def auth_headers(self, *, accept: str = "application/json") -> dict[str, str]:
        token = self.credential.get_token(GRAPH_SCOPE).token
        headers = {"Authorization": f"Bearer {token}"}
        if accept:
            headers["Accept"] = accept
        return headers

    def request_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.settings.base_url}/{path.lstrip('/')}"
        for attempt in range(_MAX_RETRIES + 1):
            response = self.session.get(
                url,
                headers=self.auth_headers(),
                params=params,
                timeout=self.settings.timeout_seconds,
            )
            if response.status_code == 429 and attempt < _MAX_RETRIES:
                delay = _retry_delay(response, attempt)
                _logger.warning(
                    "Graph API rate-limited (429); retrying in %.1fs (attempt %d/%d)",
                    delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)
                continue
            if not response.ok:
                raise RuntimeError(graph_error_message(response))
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected Graph payload for {path}: {payload!r}")
            return payload
        raise RuntimeError(f"Graph API rate limit exceeded after {_MAX_RETRIES} retries for {path}")

    def request_bytes(self, path: str) -> bytes:
        url = f"{self.settings.base_url}/{path.lstrip('/')}"
        for attempt in range(_MAX_RETRIES + 1):
            response = self.session.get(
                url,
                headers=self.auth_headers(accept="*/*"),
                timeout=self.settings.timeout_seconds,
                allow_redirects=True,
            )
            if response.status_code == 429 and attempt < _MAX_RETRIES:
                delay = _retry_delay(response, attempt)
                _logger.warning(
                    "Graph API rate-limited (429); retrying in %.1fs (attempt %d/%d)",
                    delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)
                continue
            if not response.ok:
                raise RuntimeError(graph_error_message(response))
            return response.content
        raise RuntimeError(f"Graph API rate limit exceeded after {_MAX_RETRIES} retries for {path}")

    def resolve_site_id(self, site_url: str) -> str:
        site = parse_site_url(site_url)
        payload = self.request_json(f"/sites/{site.hostname}:{quote(site.path, safe='/')}")
        site_id = text_or_blank(payload.get("id"))
        if not site_id:
            raise RuntimeError(f"Could not resolve Graph site id for {site_url}")
        return site_id

    def resolve_drive_id(
        self,
        *,
        site_id: str,
        drive_id: str = "",
        drive_name: str = "",
    ) -> str:
        if drive_id:
            return drive_id

        if drive_name:
            payload = self.request_json(f"/sites/{site_id}/drives")
            drives = payload.get("value", [])
            if isinstance(drives, list):
                for drive in drives:
                    if not isinstance(drive, dict):
                        continue
                    if text_or_blank(drive.get("name")).lower() == drive_name.lower():
                        resolved_drive_id = text_or_blank(drive.get("id"))
                        if resolved_drive_id:
                            return resolved_drive_id

            available_names = ", ".join(
                sorted(
                    text_or_blank(drive.get("name"))
                    for drive in drives
                    if isinstance(drive, dict) and text_or_blank(drive.get("name"))
                )
            )
            raise RuntimeError(
                f"Graph drive '{drive_name}' was not found on site {site_id}. Available drives: {available_names or 'none'}"
            )

        payload = self.request_json(f"/sites/{site_id}/drive")
        resolved_drive_id = text_or_blank(payload.get("id"))
        if not resolved_drive_id:
            raise RuntimeError(f"Could not resolve the default drive for site {site_id}")
        return resolved_drive_id

    def get_drive_item(
        self,
        *,
        drive_id: str,
        file_path: str = "",
        item_id: str = "",
    ) -> dict[str, Any]:
        if item_id:
            return self.request_json(f"/drives/{drive_id}/items/{item_id}")

        item_path = normalize_item_path(file_path)
        return self.request_json(f"/drives/{drive_id}/root:{quote(item_path, safe='/')}")

    def get_drive_item_content(
        self,
        *,
        drive_id: str,
        file_path: str = "",
        item_id: str = "",
    ) -> bytes:
        if item_id:
            return self.request_bytes(f"/drives/{drive_id}/items/{item_id}/content")

        item_path = normalize_item_path(file_path)
        return self.request_bytes(f"/drives/{drive_id}/root:{quote(item_path, safe='/')}:/content")

    def describe_file(
        self,
        *,
        site_url: str,
        drive_id: str = "",
        drive_name: str = "",
        file_path: str = "",
        item_id: str = "",
    ) -> dict[str, Any]:
        site_id = self.resolve_site_id(site_url)
        resolved_drive_id = self.resolve_drive_id(
            site_id=site_id,
            drive_id=drive_id,
            drive_name=drive_name,
        )
        item = self.get_drive_item(
            drive_id=resolved_drive_id,
            file_path=file_path,
            item_id=item_id,
        )
        return {
            "site_id": site_id,
            "drive_id": resolved_drive_id,
            "item_id": text_or_blank(item.get("id")),
            "name": text_or_blank(item.get("name")),
            "web_url": text_or_blank(item.get("webUrl")),
            "etag": text_or_blank(item.get("eTag")),
            "ctag": text_or_blank(item.get("cTag")),
            "last_modified": text_or_blank(item.get("lastModifiedDateTime")),
            "size": item.get("size"),
        }

    def download_file(
        self,
        *,
        site_url: str,
        drive_id: str = "",
        drive_name: str = "",
        file_path: str = "",
        item_id: str = "",
    ) -> GraphDownloadedFile:
        site_id = self.resolve_site_id(site_url)
        resolved_drive_id = self.resolve_drive_id(
            site_id=site_id,
            drive_id=drive_id,
            drive_name=drive_name,
        )
        item = self.get_drive_item(
            drive_id=resolved_drive_id,
            file_path=file_path,
            item_id=item_id,
        )
        content = self.get_drive_item_content(
            drive_id=resolved_drive_id,
            file_path=file_path,
            item_id=item_id,
        )
        return GraphDownloadedFile(
            name=text_or_blank(item.get("name")) or text_or_blank(file_path) or text_or_blank(item_id),
            content=content,
            content_type=text_or_blank(
                item.get("file", {}).get("mimeType") if isinstance(item.get("file"), dict) else ""
            ),
            site_id=site_id,
            drive_id=resolved_drive_id,
            item_id=text_or_blank(item.get("id")),
            web_url=text_or_blank(item.get("webUrl")),
        )
