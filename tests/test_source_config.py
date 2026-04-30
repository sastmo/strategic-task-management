from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.infrastructure.sources import expand_source_specs, parse_source_config


class SourceConfigTests(unittest.TestCase):
    def test_parse_source_config_accepts_comma_separated_sources(self) -> None:
        config = parse_source_config("/tmp/a.csv, /tmp/b.xlsx")

        self.assertEqual(config.sources, ["/tmp/a.csv", "/tmp/b.xlsx"])
        self.assertEqual(config.union_mode, "union")

    def test_directory_expansion_preserves_explicit_source_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            (directory / "alpha.csv").write_text("name,currentImpact,futureImpact,progress\nA,1,2,3\n", encoding="utf-8")
            (directory / "beta.json").write_text("[]", encoding="utf-8")

            sources = expand_source_specs(
                [
                    {
                        "source": str(directory),
                        "source_name": "shared_feed",
                        "source_priority": 250,
                    }
                ]
            )

        self.assertEqual(len(sources), 2)
        self.assertEqual({source.source_name for source in sources}, {"shared_feed"})
        self.assertEqual([source.source_order for source in sources], [1, 2])

    def test_source_root_rejects_glob_matches_outside_allowed_directory(self) -> None:
        with tempfile.TemporaryDirectory() as allowed_dir, tempfile.TemporaryDirectory() as outside_dir:
            allowed_root = Path(allowed_dir)
            outside_file = Path(outside_dir) / "tasks.csv"
            outside_file.write_text(
                "name,currentImpact,futureImpact,progress\nA,1,2,3\n",
                encoding="utf-8",
            )

            with patch.dict("os.environ", {"TASK_SOURCE_ROOT": str(allowed_root)}, clear=False):
                with self.assertRaises(ValueError):
                    expand_source_specs([{"glob": str(outside_file)}])

    def test_source_root_allows_directory_expansion_within_allowed_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)
            (workspace / "tasks.csv").write_text(
                "name,currentImpact,futureImpact,progress\nA,1,2,3\n",
                encoding="utf-8",
            )

            with patch.dict("os.environ", {"TASK_SOURCE_ROOT": str(workspace)}, clear=False):
                sources = expand_source_specs([str(workspace)])

        self.assertEqual(len(sources), 1)
        self.assertEqual(Path(sources[0].source).name, "tasks.csv")


if __name__ == "__main__":
    unittest.main()
