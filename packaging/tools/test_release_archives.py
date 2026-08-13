#!/usr/bin/env python3
"""Deterministic tests for the public release and archive contracts."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import shlex
import tempfile
import unittest
import zipfile

from build_release_archive import build_archive
from release_contract import (
    ANDROID_JNI_ABIS,
    ANDROID_JNI_BUNDLE,
    PACKAGE_VERSION,
    RELEASE_TARGETS,
    VERSION_ASSET,
    ReleaseContractError,
    archive_asset_names,
    public_asset_names,
    verify_public_assets,
    write_version_asset,
)
from verify_release_archive import ReleaseArchiveError, verify_archive


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]
TEST_EPOCH = 1_700_000_000


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_stage(root: pathlib.Path, target_index: int) -> pathlib.Path:
    target = RELEASE_TARGETS[target_index]
    stage = root / target.package
    for relative in sorted(target.expected_files):
        path = stage / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{target.rust_target}:{relative}\n".encode())
    return stage


def write_test_version_asset(root: pathlib.Path) -> pathlib.Path:
    return write_version_asset(
        root,
        tag=f"v{PACKAGE_VERSION}",
        commit="0" * 40,
        repository="example/mptunnel",
    )


class ReleaseArchiveTests(unittest.TestCase):
    def test_every_target_archive_is_byte_reproducible_and_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            for index, target in enumerate(RELEASE_TARGETS):
                with self.subTest(target=target.rust_target):
                    stage = make_stage(root, index)
                    first = root / "first" / target.archive_name
                    second = root / "second" / target.archive_name
                    build_archive(stage, first, target, TEST_EPOCH)
                    build_archive(stage, second, target, TEST_EPOCH)
                    self.assertEqual(sha256(first), sha256(second))
                    verify_archive(first, target)
                    verify_archive(second, target)

    def test_android_jni_archive_is_exact_reproducible_and_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            stage = root / ANDROID_JNI_BUNDLE.package
            for relative in sorted(ANDROID_JNI_BUNDLE.expected_files):
                path = stage / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(f"android-jni:{relative}\n".encode())
            first = root / "first" / ANDROID_JNI_BUNDLE.archive_name
            second = root / "second" / ANDROID_JNI_BUNDLE.archive_name
            build_archive(stage, first, ANDROID_JNI_BUNDLE, TEST_EPOCH)
            build_archive(stage, second, ANDROID_JNI_BUNDLE, TEST_EPOCH)
            self.assertEqual(sha256(first), sha256(second))
            verify_archive(first, ANDROID_JNI_BUNDLE)
            verify_archive(second, ANDROID_JNI_BUNDLE)
            self.assertEqual(ANDROID_JNI_ABIS, ("arm64-v8a", "x86_64"))
            self.assertEqual(
                ANDROID_JNI_BUNDLE.expected_files,
                frozenset(
                    {
                        "LICENSE",
                        "README.md",
                        "arm64-v8a/libmptunnel.so",
                        "x86_64/libmptunnel.so",
                    }
                ),
            )

    def test_archive_verifier_rejects_extra_ci_payload(self) -> None:
        target = next(target for target in RELEASE_TARGETS if target.os == "macos")
        index = RELEASE_TARGETS.index(target)
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            stage = make_stage(root, index)
            archive = root / target.archive_name
            build_archive(stage, archive, target, TEST_EPOCH)
            with zipfile.ZipFile(archive, "a") as bundle:
                bundle.writestr(f"{target.package}/raw-ci-log.txt", b"not public\n")
            with self.assertRaises(ReleaseArchiveError):
                verify_archive(archive, target)

    def test_stage_builder_rejects_unknown_files(self) -> None:
        target = RELEASE_TARGETS[0]
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            stage = make_stage(root, 0)
            (stage / "build-evidence.json").write_text("{}\n", encoding="utf-8")
            with self.assertRaises(ReleaseContractError):
                build_archive(
                    stage,
                    root / target.archive_name,
                    target,
                    TEST_EPOCH,
                )

    def test_public_inventory_has_version_metadata_and_eight_archives(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            for name in archive_asset_names():
                (root / name).write_bytes(f"archive:{name}\n".encode())
            version_asset = write_test_version_asset(root)
            self.assertEqual(len(archive_asset_names()), 8)
            self.assertEqual(len(public_asset_names()), 9)
            self.assertEqual(version_asset.name, VERSION_ASSET)
            self.assertIn(VERSION_ASSET, public_asset_names())
            self.assertNotIn("SHA256SUMS", public_asset_names())
            self.assertTrue(
                all(
                    name.startswith(f"mptunnel-{PACKAGE_VERSION}-")
                    for name in archive_asset_names()
                )
            )
            metadata = json.loads(version_asset.read_text(encoding="utf-8"))
            self.assertEqual(
                set(metadata),
                {"schema_version", "product", "version", "tag", "commit", "assets"},
            )
            self.assertEqual(
                [set(asset) for asset in metadata["assets"]],
                [{"name", "download_url"}] * 8,
            )
            verify_public_assets(root)

            (root / "raw-actions-artifact.zip").write_bytes(b"private staging\n")
            with self.assertRaises(ReleaseContractError):
                verify_public_assets(root)

    def test_platform_helpers_are_explicit_and_non_overlapping(self) -> None:
        by_os = {target.os: target for target in RELEASE_TARGETS}
        self.assertIn(
            "service/systemd/mptunnel.service",
            by_os["linux"].expected_files,
        )
        self.assertEqual(by_os["macos"].platform_files, frozenset())
        self.assertIn("wintun.dll", by_os["windows"].expected_files)
        self.assertNotIn(
            "service/systemd/mptunnel.service",
            by_os["windows"].expected_files,
        )
        self.assertNotIn("wintun.dll", by_os["android"].expected_files)

    def test_systemd_template_passes_only_the_canonical_config_path(self) -> None:
        systemd = (
            REPOSITORY_ROOT / "packaging/service/systemd/mptunnel.service"
        ).read_text(encoding="utf-8")
        exec_start = next(
            line.removeprefix("ExecStart=")
            for line in systemd.splitlines()
            if line.startswith("ExecStart=")
        )
        self.assertEqual(
            shlex.split(exec_start),
            [
                "/usr/local/bin/mptunnel",
                "--config",
                "/etc/mptunnel/config.toml",
            ],
        )

    def test_packaging_sources_and_documented_names_match_contract(self) -> None:
        required_sources = (
            "LICENSE",
            "packaging/README.md",
            "packaging/android/README.md",
            "packaging/android/package-jni-release.sh",
            "packaging/service/systemd/mptunnel.service",
            "examples/client.toml",
            "examples/server.toml",
        )
        for relative in required_sources:
            with self.subTest(source=relative):
                self.assertTrue((REPOSITORY_ROOT / relative).is_file())

        package_readme = (REPOSITORY_ROOT / "packaging/README.md").read_text(
            encoding="utf-8"
        )
        for asset in archive_asset_names():
            documented = asset.replace(PACKAGE_VERSION, "<version>", 1)
            self.assertIn(f"`{documented}`", package_readme)
        for target in RELEASE_TARGETS:
            self.assertNotIn(target.rust_target, package_readme)

    def test_publish_workflow_never_releases_raw_actions_artifacts(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/release.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn('gh release create "$RELEASE_TAG" artifacts/*', workflow)
        self.assertNotIn("dist/*.sha256", workflow)
        self.assertNotIn("write-checksums", workflow)
        self.assertNotIn("--with-checksums", workflow)
        self.assertIn("write-version", workflow)
        self.assertIn('--repository "$GITHUB_REPOSITORY"', workflow)
        self.assertIn("packaging/tools/release_contract.py verify-assets", workflow)
        self.assertIn('"${release_assets[@]}"', workflow)
        self.assertIn("Preflight existing release state", workflow)
        self.assertIn("state=absent", workflow)
        self.assertIn('"draft" else "published"', workflow)
        self.assertIn("immutable published release is a verification", workflow)
        self.assertIn("Published artifacts are immutable release inputs", workflow)
        self.assertIn("existing draft asset differs from this build", workflow)
        self.assertIn("Complete matching draft payload", workflow)
        self.assertIn("missing-draft-assets.txt", workflow)
        self.assertIn("release-created-by-run", workflow)
        self.assertIn(
            "always() && (failure() || cancelled())",
            workflow,
        )

    def test_all_github_build_workflows_share_the_exact_target_and_android_pins(
        self,
    ) -> None:
        expected_targets = {target.rust_target for target in RELEASE_TARGETS}
        for name in ("ci.yml", "release-check.yml", "release.yml"):
            with self.subTest(workflow=name):
                workflow = (REPOSITORY_ROOT / ".github/workflows" / name).read_text(
                    encoding="utf-8"
                )
                configured_targets = set(
                    re.findall(r"^\s+target:\s+(\S+)\s*$", workflow, re.MULTILINE)
                )
                self.assertEqual(configured_targets, expected_targets)
                self.assertIn('ndk_version="27.3.13750724"', workflow)
                self.assertIn("aarch64-linux-android21-clang", workflow)
                if name != "ci.yml":
                    self.assertIn(
                        "macOS release has a non-system dependency",
                        workflow,
                    )
                    self.assertIn(
                        "Android release has a non-NDK/system dependency",
                        workflow,
                    )
                    self.assertIn(
                        "Windows release has a non-system import",
                        workflow,
                    )
                    self.assertIn("API-MS-WIN-(?!CRT-)", workflow)
                    self.assertIn(
                        "Wintun must remain a bundled runtime-loaded DLL",
                        workflow,
                    )

        ci_workflow = (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("actions/upload-artifact", ci_workflow)
        self.assertNotIn("gh release", ci_workflow)

    def test_android_jni_release_build_is_actions_owned_and_two_abi_only(
        self,
    ) -> None:
        helper = (
            REPOSITORY_ROOT / "packaging/android/build-jni-libs.sh"
        ).read_text(encoding="utf-8")
        self.assertIn(
            '"arm64-v8a:aarch64-linux-android:aarch64-linux-android"', helper
        )
        self.assertIn(
            '"x86_64:x86_64-linux-android:x86_64-linux-android"', helper
        )
        self.assertNotIn("armv7-linux-androideabi", helper)
        self.assertNotIn("i686-linux-android", helper)
        self.assertEqual(
            helper.count("Java_com_v2ray_ang_mpp_MptunnelNative_"),
            7,
        )
        self.assertIn("llvm-strip", helper)
        self.assertIn("llvm-nm", helper)
        self.assertIn("0x4000", helper)

        for name in ("release-check.yml", "release.yml"):
            with self.subTest(workflow=name):
                workflow = (
                    REPOSITORY_ROOT / ".github/workflows" / name
                ).read_text(encoding="utf-8")
                self.assertIn("android-jni:", workflow)
                self.assertIn(
                    "rustup target add aarch64-linux-android x86_64-linux-android",
                    workflow,
                )
                self.assertIn('ndk_version="27.3.13750724"', workflow)
                self.assertIn('echo "ANDROID_NDK_HOME=${ndk_root}"', workflow)
                self.assertIn("packaging/android/build-jni-libs.sh", workflow)
                self.assertIn("packaging/android/package-jni-release.sh", workflow)


if __name__ == "__main__":
    unittest.main()
