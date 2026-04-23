from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
