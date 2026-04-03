from __future__ import annotations


def has_azure_identity_support() -> bool:
    try:
        import azure.identity  # noqa: F401
    except ImportError:
        return False

    return True


def get_default_azure_credential():
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        raise RuntimeError(
            "azure-identity is required to use Azure managed identity features."
        ) from exc

    return DefaultAzureCredential(exclude_interactive_browser_credential=False)
