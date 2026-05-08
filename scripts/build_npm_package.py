#!/usr/bin/env python3
"""Stage and optionally pack npm packages for claude-app-server."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NPM_PACKAGE_ROOT = REPO_ROOT
NPM_NAME = "@logitropic/claude-app-server"

PLATFORM_PACKAGES: dict[str, dict[str, str]] = {
    "claude-app-server-linux-x64": {
        "npm_name": "@logitropic/claude-app-server-linux-x64",
        "npm_tag": "linux-x64",
        "target_triple": "x86_64-unknown-linux-musl",
        "os": "linux",
        "cpu": "x64",
    },
    "claude-app-server-linux-arm64": {
        "npm_name": "@logitropic/claude-app-server-linux-arm64",
        "npm_tag": "linux-arm64",
        "target_triple": "aarch64-unknown-linux-musl",
        "os": "linux",
        "cpu": "arm64",
    },
    "claude-app-server-darwin-x64": {
        "npm_name": "@logitropic/claude-app-server-darwin-x64",
        "npm_tag": "darwin-x64",
        "target_triple": "x86_64-apple-darwin",
        "os": "darwin",
        "cpu": "x64",
    },
    "claude-app-server-darwin-arm64": {
        "npm_name": "@logitropic/claude-app-server-darwin-arm64",
        "npm_tag": "darwin-arm64",
        "target_triple": "aarch64-apple-darwin",
        "os": "darwin",
        "cpu": "arm64",
    },
    "claude-app-server-win32-x64": {
        "npm_name": "@logitropic/claude-app-server-win32-x64",
        "npm_tag": "win32-x64",
        "target_triple": "x86_64-pc-windows-msvc",
        "os": "win32",
        "cpu": "x64",
    },
    "claude-app-server-win32-arm64": {
        "npm_name": "@logitropic/claude-app-server-win32-arm64",
        "npm_tag": "win32-arm64",
        "target_triple": "aarch64-pc-windows-msvc",
        "os": "win32",
        "cpu": "arm64",
    },
}

PACKAGE_EXPANSIONS = {
    "claude-app-server": ["claude-app-server", *PLATFORM_PACKAGES.keys()],
}

PACKAGE_CHOICES = tuple(PACKAGE_EXPANSIONS["claude-app-server"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package",
        choices=PACKAGE_CHOICES,
        default="claude-app-server",
        help="Which npm package to stage.",
    )
    parser.add_argument("--version", help="Version to write to package.json.")
    parser.add_argument("--release-version", help="Release version to stage.")
    parser.add_argument(
        "--staging-dir",
        type=Path,
        help="Empty directory where package contents should be staged.",
    )
    parser.add_argument(
        "--pack-output",
        type=Path,
        help="Path where npm pack output should be written.",
    )
    parser.add_argument(
        "--vendor-src",
        type=Path,
        help="Directory containing vendor/<target-triple>/claude-app-server binaries.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    version = args.release_version or args.version
    if args.release_version and args.version and args.release_version != args.version:
        raise RuntimeError("--version and --release-version must match when both are provided.")
    if not version:
        raise RuntimeError("Must specify --version or --release-version.")

    staging_dir, created_temp = prepare_staging_dir(args.staging_dir)
    try:
        stage_sources(staging_dir, version, args.package, args.vendor_src)
        print(f"Staged {args.package} {version} in {staging_dir}")

        if args.pack_output is not None:
            output_path = run_npm_pack(staging_dir, args.pack_output)
            print(f"npm pack output written to {output_path}")
    finally:
        if created_temp:
            # Preserve temp staging dirs for inspection, matching Codex's release helper.
            pass

    return 0


def prepare_staging_dir(staging_dir: Path | None) -> tuple[Path, bool]:
    if staging_dir is None:
        return Path(tempfile.mkdtemp(prefix="claude-app-server-npm-stage-")), True

    staging_dir = staging_dir.resolve()
    staging_dir.mkdir(parents=True, exist_ok=True)
    if any(staging_dir.iterdir()):
        raise RuntimeError(f"Staging directory {staging_dir} is not empty.")
    return staging_dir, False


def stage_sources(
    staging_dir: Path,
    version: str,
    package: str,
    vendor_src: Path | None,
) -> None:
    if package == "claude-app-server":
        stage_root_package(staging_dir, version)
        return

    platform_package = PLATFORM_PACKAGES[package]
    if vendor_src is None:
        raise RuntimeError(f"--vendor-src is required when staging {package}.")

    stage_platform_package(staging_dir, version, platform_package, vendor_src)


def stage_root_package(staging_dir: Path, version: str) -> None:
    bin_dir = staging_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(NPM_PACKAGE_ROOT / "bin" / "claude-app-server.js", bin_dir)

    copy_if_exists(REPO_ROOT / "README.md", staging_dir / "README.md")

    with open(NPM_PACKAGE_ROOT / "package.json", "r", encoding="utf-8") as fh:
        package_json = json.load(fh)

    package_json["version"] = version
    package_json["files"] = ["bin"]
    package_json["optionalDependencies"] = {
        platform_package["npm_name"]: (
            f"npm:{NPM_NAME}@"
            f"{compute_platform_package_version(version, platform_package['npm_tag'])}"
        )
        for platform_package in PLATFORM_PACKAGES.values()
    }

    write_package_json(staging_dir, package_json)


def stage_platform_package(
    staging_dir: Path,
    version: str,
    platform_package: dict[str, str],
    vendor_src: Path,
) -> None:
    platform_version = compute_platform_package_version(version, platform_package["npm_tag"])
    target_triple = platform_package["target_triple"]

    source_dir = vendor_src.resolve() / target_triple / "claude-app-server"
    if not source_dir.exists():
        raise RuntimeError(f"Missing native binary directory: {source_dir}")

    vendor_dest = staging_dir / "vendor" / target_triple / "claude-app-server"
    vendor_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, vendor_dest)

    copy_if_exists(REPO_ROOT / "README.md", staging_dir / "README.md")

    with open(NPM_PACKAGE_ROOT / "package.json", "r", encoding="utf-8") as fh:
        root_package_json = json.load(fh)

    package_json = {
        "name": NPM_NAME,
        "version": platform_version,
        "description": root_package_json.get("description"),
        "license": root_package_json.get("license", "MIT"),
        "os": [platform_package["os"]],
        "cpu": [platform_package["cpu"]],
        "files": ["vendor"],
        "publishConfig": root_package_json.get("publishConfig", {"access": "public"}),
        "repository": root_package_json.get("repository"),
    }

    engines = root_package_json.get("engines")
    if isinstance(engines, dict):
        package_json["engines"] = engines

    write_package_json(staging_dir, package_json)


def compute_platform_package_version(version: str, platform_tag: str) -> str:
    return f"{version}-{platform_tag}"


def copy_if_exists(src: Path, dest: Path) -> None:
    if src.exists():
        shutil.copy2(src, dest)


def write_package_json(staging_dir: Path, package_json: dict) -> None:
    with open(staging_dir / "package.json", "w", encoding="utf-8") as out:
        json.dump(package_json, out, indent=2)
        out.write("\n")


def run_npm_pack(staging_dir: Path, output_path: Path) -> Path:
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="claude-app-server-npm-pack-") as pack_dir_str:
        pack_dir = Path(pack_dir_str)
        stdout = subprocess.check_output(
            ["npm", "pack", "--json", "--pack-destination", str(pack_dir)],
            cwd=staging_dir,
            text=True,
        )
        pack_output = json.loads(stdout)
        if not pack_output:
            raise RuntimeError("npm pack did not produce a tarball.")

        tarball_name = pack_output[0].get("filename") or pack_output[0].get("name")
        if not tarball_name:
            raise RuntimeError("Unable to determine npm pack output filename.")

        tarball_path = pack_dir / tarball_name
        if not tarball_path.exists():
            raise RuntimeError(f"Expected npm pack output not found: {tarball_path}")

        shutil.move(str(tarball_path), output_path)

    return output_path


if __name__ == "__main__":
    raise SystemExit(main())
