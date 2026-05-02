from __future__ import annotations

import base64
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import quote

from src.domain.identity import (
    AuthenticatedUser,
    build_user_key,
    normalize_email,
    normalize_role_collection,
)
from src.domain.tasks import text_or_blank

CLIENT_PRINCIPAL_HEADER = "x-ms-client-principal"
CLIENT_PRINCIPAL_ID_HEADER = "x-ms-client-principal-id"
CLIENT_PRINCIPAL_NAME_HEADER = "x-ms-client-principal-name"
CLIENT_PRINCIPAL_IDP_HEADER = "x-ms-client-principal-idp"

EMAIL_CLAIM_KEYS = (
    "preferred_username",
    "email",
    "emails",
    "upn",
    "unique_name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier",
)
DISPLAY_NAME_CLAIM_KEYS = (
    "name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
)
PRINCIPAL_ID_CLAIM_KEYS = (
    "oid",
    "objectidentifier",
    "http://schemas.microsoft.com/identity/claims/objectidentifier",
)
TENANT_ID_CLAIM_KEYS = (
    "tid",
    "tenantid",
    "http://schemas.microsoft.com/identity/claims/tenantid",
)
GROUP_CLAIM_KEYS = (
    "groups",
    "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups",
)
ROLE_CLAIM_KEYS = (
    "roles",
    "role",
    "http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
)
_PROVIDER_ALIASES = {
    "aad": {"aad", "azureactivedirectory"},
}


def normalize_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}

    for key, value in headers.items():
        header_name = text_or_blank(key).lower()
        if not header_name:
            continue

        if isinstance(value, list | tuple):
            header_value = next(
                (text_or_blank(item) for item in value if text_or_blank(item)),
                "",
            )
        else:
            header_value = text_or_blank(value)

        normalized[header_name] = header_value

    return normalized


def decode_client_principal(encoded_value: str) -> dict[str, Any]:
    value = text_or_blank(encoded_value)
    if not value:
        return {}

    padded = value + ("=" * (-len(value) % 4))
    decoded = base64.urlsafe_b64decode(padded.encode("utf-8"))
    payload = json.loads(decoded.decode("utf-8"))

    if not isinstance(payload, dict):
        raise ValueError("App Service principal payload must be a JSON object.")

    return payload


def claim_index(payload: Mapping[str, Any]) -> dict[str, list[str]]:
    indexed: dict[str, list[str]] = defaultdict(list)

    for claim in payload.get("claims", []):
        if not isinstance(claim, Mapping):
            continue

        claim_type = text_or_blank(claim.get("typ"))
        claim_value = text_or_blank(claim.get("val"))
        if not claim_type or not claim_value:
            continue

        claim_type_lower = claim_type.lower()
        indexed[claim_type_lower].append(claim_value)

        short_name = claim_type_lower.rsplit("/", 1)[-1]
        if short_name != claim_type_lower:
            indexed[short_name].append(claim_value)

    return indexed


def first_claim_value(indexed_claims: Mapping[str, list[str]], keys: Iterable[str]) -> str:
    for key in keys:
        values = indexed_claims.get(key.lower(), [])
        for value in values:
            cleaned = text_or_blank(value)
            if cleaned:
                return cleaned
    return ""


def all_claim_values(indexed_claims: Mapping[str, list[str]], keys: Iterable[str]) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()

    for key in keys:
        for value in indexed_claims.get(key.lower(), []):
            cleaned = text_or_blank(value)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                values.append(cleaned)

    return tuple(values)


def parse_app_service_user(headers: Mapping[str, Any]) -> AuthenticatedUser | None:
    normalized_headers = normalize_headers(headers)
    principal_payload = decode_client_principal(
        normalized_headers.get(CLIENT_PRINCIPAL_HEADER, "")
    )
    indexed_claims = claim_index(principal_payload)

    email = normalize_email(
        first_claim_value(indexed_claims, EMAIL_CLAIM_KEYS)
        or normalized_headers.get(CLIENT_PRINCIPAL_NAME_HEADER, "")
    )
    principal_id = (
        text_or_blank(normalized_headers.get(CLIENT_PRINCIPAL_ID_HEADER, ""))
        or first_claim_value(indexed_claims, PRINCIPAL_ID_CLAIM_KEYS)
    )
    tenant_id = first_claim_value(indexed_claims, TENANT_ID_CLAIM_KEYS)
    display_name = (
        first_claim_value(indexed_claims, DISPLAY_NAME_CLAIM_KEYS)
        or email
        or principal_id
    )
    identity_provider_header = text_or_blank(normalized_headers.get(CLIENT_PRINCIPAL_IDP_HEADER, ""))
    identity_provider_payload = text_or_blank(principal_payload.get("auth_typ"))
    if (
        identity_provider_header
        and identity_provider_payload
        and identity_provider_header.lower() != identity_provider_payload.lower()
    ):
        raise ValueError("App Service identity provider headers disagree with the principal payload.")
    identity_provider = identity_provider_header or identity_provider_payload
    groups = all_claim_values(indexed_claims, GROUP_CLAIM_KEYS)
    app_roles = normalize_role_collection(all_claim_values(indexed_claims, ROLE_CLAIM_KEYS))

    if not email and not principal_id:
        return None

    return AuthenticatedUser(
        user_key=build_user_key(
            email=email,
            principal_id=principal_id,
            tenant_id=tenant_id,
        ),
        email=email,
        display_name=display_name,
        principal_id=principal_id,
        tenant_id=tenant_id,
        identity_provider=identity_provider or "aad",
        auth_source="app_service",
        groups=groups,
        app_roles=app_roles,
        is_authenticated=True,
    )


def identity_provider_allowed(identity_provider: str, configured_provider: str) -> bool:
    normalized_identity_provider = text_or_blank(identity_provider).lower()
    normalized_configured_provider = text_or_blank(configured_provider).lower()
    if not normalized_identity_provider or not normalized_configured_provider:
        return False

    allowed_values = _PROVIDER_ALIASES.get(
        normalized_configured_provider,
        {normalized_configured_provider},
    )
    return normalized_identity_provider in allowed_values


def build_app_service_login_url(provider: str = "aad", redirect_path: str = "/") -> str:
    return f"/.auth/login/{provider}?post_login_redirect_uri={quote(redirect_path, safe='/')}"


def build_app_service_logout_url(redirect_path: str = "/") -> str:
    return f"/.auth/logout?post_logout_redirect_uri={quote(redirect_path, safe='/')}"
