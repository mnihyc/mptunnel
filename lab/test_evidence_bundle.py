import tempfile
import unittest
from pathlib import Path

from evidence_bundle import EvidenceBundleError, seal, verify


class EvidenceBundleTests(unittest.TestCase):
    def test_seal_and_verify_bind_every_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run-1").mkdir()
            (root / "run-1" / "results.jsonl").write_text(
                '{"status":"ok"}\n', encoding="utf-8"
            )
            (root / "summary.json").write_text("{}\n", encoding="utf-8")

            sealed = seal(root)
            verified = verify(root)

            self.assertEqual(sealed, verified)
            self.assertEqual(sealed["artifact_count"], 2)
            self.assertEqual(
                [row["path"] for row in sealed["artifacts"]],
                ["run-1/results.jsonl", "summary.json"],
            )

    def test_mutation_or_extra_artifact_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "results.jsonl"
            artifact.write_text('{"status":"ok"}\n', encoding="utf-8")
            seal(root)

            artifact.write_text('{"status":"fail"}\n', encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "artifacts differ"):
                verify(root)

            artifact.write_text('{"status":"ok"}\n', encoding="utf-8")
            (root / "late.log").write_text("late\n", encoding="utf-8")
            with self.assertRaisesRegex(EvidenceBundleError, "artifacts differ"):
                verify(root)

    def test_existing_seal_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result").write_text("one", encoding="utf-8")
            seal(root)

            with self.assertRaisesRegex(EvidenceBundleError, "refusing to overwrite"):
                seal(root)

    def test_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "result"
            artifact.write_text("one", encoding="utf-8")
            (root / "alias").symlink_to(artifact)

            with self.assertRaisesRegex(EvidenceBundleError, "symlinks are forbidden"):
                seal(root)


if __name__ == "__main__":
    unittest.main()
