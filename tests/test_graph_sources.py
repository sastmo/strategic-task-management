from __future__ import annotations

import unittest
from unittest.mock import patch

from src.infrastructure.graph.client import load_graph_auth_settings, parse_site_url
from src.infrastructure.sources import detect_source_kind, normalize_source_spec


class GraphSourceTests(unittest.TestCase):
    def test_normalize_source_spec_supports_graph_source_dict(self) -> None:
        source_spec = normalize_source_spec(
            {
                "kind": "graph",
                "site_url": "https://contoso.sharepoint.com/sites/Strategy",
                "drive_name": "Shared Documents",
                "file_path": "/Plans/master.xlsx",
            }
        )

        self.assertEqual(source_spec.kind, "graph")
        self.assertEqual(source_spec.site_url, "https://contoso.sharepoint.com/sites/Strategy")
        self.assertEqual(source_spec.drive_name, "Shared Documents")
        self.assertEqual(source_spec.file_path, "/Plans/master.xlsx")
        self.assertTrue(source_spec.source.startswith("graph://"))
        self.assertEqual(source_spec.source_name, "master")

    def test_detect_source_kind_recognizes_graph_identifier(self) -> None:
        self.assertEqual(
            detect_source_kind("graph://contoso.sharepoint.com/sites/Strategy::Documents::Plans/master.xlsx"),
            "graph",
        )

    def test_parse_site_url_extracts_hostname_and_path(self) -> None:
        site_reference = parse_site_url("https://contoso.sharepoint.com/sites/Strategy")

        self.assertEqual(site_reference.hostname, "contoso.sharepoint.com")
        self.assertEqual(site_reference.path, "/sites/Strategy")

    def test_load_graph_auth_settings_prefers_client_secret_when_present(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "GRAPH_TENANT_ID": "tenant-1",
                "GRAPH_CLIENT_ID": "client-1",
                "GRAPH_CLIENT_SECRET": "secret-1",
            },
            clear=False,
        ):
            settings = load_graph_auth_settings()

        self.assertEqual(settings.auth_mode, "client_secret")
        self.assertEqual(settings.tenant_id, "tenant-1")
        self.assertEqual(settings.client_id, "client-1")


if __name__ == "__main__":
    unittest.main()
