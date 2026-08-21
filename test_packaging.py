"""Release packaging checks for the scanner-safe runtime bundle."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RUNTIME_FILES = ("adapter.py", "plugin.yaml", "__init__.py")


class RuntimeBundleTests(unittest.TestCase):
    def test_scanner_safe_bundle_matches_root_runtime(self):
        bundle = ROOT / "plugin"
        bundled_source_files = {
            path.relative_to(bundle).as_posix()
            for path in bundle.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        self.assertEqual(bundled_source_files, set(RUNTIME_FILES))
        for name in RUNTIME_FILES:
            with self.subTest(name=name):
                self.assertEqual(
                    (bundle / name).read_bytes(),
                    (ROOT / name).read_bytes(),
                    f"plugin/{name} must match the reviewed root runtime file",
                )


if __name__ == "__main__":
    unittest.main()
