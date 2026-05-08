#!/usr/bin/env python3
"""Stage claude-app-server npm tarballs for release."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_npm_package.py"

_SPEC = importlib.util.spec_from_file_location("claude_app_server_build_npm_package", BUILD_SCRIPT)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load module from {BUILD_SCRIPT}")
_BUILD_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BUILD_MODULE)

PACKAGE_EXPANSIONS = getattr(_BUILD_MODULE, "PACKAGE_EXPANSIONS")
PLATFORM_PACKAGES = getattr(_BUILD_MODULE, "PLATFORM_PACKAGES")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release-version",
        required=True,
        help="Version to stage, for example 0.1.0 or 0.1.0-alpha.1.",
    )
    parser.add_argument(
        "--package",
        dest="packages",
        action="append",
        required=True,
        help="Package to stage. May be provided multiple times.",
    )
    parser.add_argument(
        "--vendor-src",
        type=Path,
        default=REPO_ROOT / "dist" / "native" / "vendor",
        help="Directory containing native vendor payloads.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "dist" / "npm",
        help="Directory where npm tarballs should be written.",
    )
    parser.add_argument(
        "--keep-staging-dirs",
        action="store_true",
        help="Retain temporary staging directories for inspection.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    packages = expand_packages(args.packages)
    runner_temp = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir()))
    final_messages: list[str] = []

    for package in packages:
        staging_dir = Path(tempfile.mkdtemp(prefix=f"npm-stage-{package}-", dir=runner_temp))
        pack_output = output_dir / tarball_name_for_package(package, args.release_version)

        cmd = [
            str(BUILD_SCRIPT),
            "--package",
            package,
            "--release-version",
            args.release_version,
            "--staging-dir",
            str(staging_dir),
            "--pack-output",
            str(pack_output),
        ]

        if package in PLATFORM_PACKAGES:
            cmd.extend(["--vendor-src", str(args.vendor_src.resolve())])

        try:
            run_command(cmd)
        finally:
            if not args.keep_staging_dirs:
                shutil.rmtree(staging_dir, ignore_errors=True)

        final_messages.append(f"Staged {package} at {pack_output}")

    for message in final_messages:
        print(message)

    return 0


def expand_packages(packages: list[str]) -> list[str]:
    expanded: list[str] = []
    for package in packages:
        for expanded_package in PACKAGE_EXPANSIONS.get(package, [package]):
            if expanded_package not in expanded:
                expanded.append(expanded_package)
    return expanded


def tarball_name_for_package(package: str, version: str) -> str:
    if package in PLATFORM_PACKAGES:
        platform = package.removeprefix("claude-app-server-")
        return f"claude-app-server-npm-{platform}-{version}.tgz"
    return f"{package}-npm-{version}.tgz"


def run_command(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
