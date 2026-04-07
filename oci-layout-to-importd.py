#!/usr/bin/python3
# coding: utf-8
# /// script
# requires-python = ">= 3.10"
# dependencies = []
# ///
import argparse
import json
import shutil
import subprocess
from platform import uname
from pathlib import Path
from contextlib import contextmanager
from typing import Final


UNAME_TO_OCI: Final[dict[str, str]] = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "armv7l": "arm",
    "s390x": "s390x",
    "ppc64le": "ppc64le",
    "riscv64": "riscv64",
}


@contextmanager
def mkdir(path: str | Path, cleanup: bool):
    if not isinstance(path, Path):
        p = Path(path)
    else:
        p = path
    p.mkdir(parents=True, exist_ok=True)
    try:
        yield p
    finally:
        if cleanup:
            shutil.rmtree(p, ignore_errors=True)


@contextmanager
def mkfile(path: str):
    if not isinstance(path, Path):
        p = Path(path)
    else:
        p = path
    p.touch()
    try:
        yield p
    finally:
        p.unlink(missing_ok=True)


def get_platform() -> str:
    u = uname()
    return f"{u.system.lower()}/{UNAME_TO_OCI[u.machine]}"


def blobs_subdir(layout: Path, digest: str) -> Path:
    algo, _, hex = digest.partition(":")
    return layout / "blobs" / algo / hex


def convert(layout: Path, platform: str, workdir: Path, image: str) -> None:
    if not (layout / "index.json"):
        raise RuntimeError("index.json not found")
    else:
        entries = json.loads((layout / "index.json").read_text())["manifests"]

    os_, _, arch = platform.partition("/")

    digest = next(
        (e["digest"] for e in entries if "image.manifest" in e["mediaType"]), None
    ) or next(
        (
            m["digest"]
            for e in entries
            if "image.index" in e["mediaType"]
            for m in json.loads(blobs_subdir(layout, e["digest"]).read_text())[
                "manifests"
            ]
            if m["platform"]["os"] == os_ and m["platform"]["architecture"] == arch
        ),
        None,
    )

    if not digest:
        raise RuntimeError("Image digest not found")

    (workdir / "v2" / image / "manifests").mkdir(parents=True, exist_ok=True)
    (workdir / "v2" / image / "blobs").mkdir(parents=True, exist_ok=True)
    latest = workdir / "v2" / image / "manifests" / "latest"
    latest.unlink(missing_ok=True)
    latest.symlink_to(blobs_subdir(layout, digest))

    manifest = json.loads(blobs_subdir(layout, digest).read_text())
    for d in [manifest["config"]["digest"]] + [l["digest"] for l in manifest["layers"]]:
        _, _, hex = d.partition(":")
        blob = workdir / "v2" / image / "blobs" / f"sha256:{hex}"
        blob.unlink(missing_ok=True)
        blob.symlink_to(blobs_subdir(layout, d))

    return None


def parse_cmdline(
    components: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(exit_on_error=False)

    parser.add_argument("layout", type=Path, help="OCI image directory")
    parser.add_argument("image", nargs="?", help="Target image name")
    parser.add_argument("-c", "--convert", action="store_true", help="Convert only")
    parser.add_argument("-p", "--platform", help="Platform in manifest")
    parser.add_argument("-w", "--workdir", help="Working directory")

    if components:
        return parser.parse_args(components)

    return parser.parse_args()


def main() -> None:
    args = parse_cmdline()

    if not (args.layout / "oci-layout").is_file():
        raise RuntimeError(f"{args.layout} is not a valid OCI image")
    else:
        layout = args.layout.resolve()

    if args.image:
        image = args.image
    else:
        image = layout.name

    if args.workdir:
        workdir = args.workdir.resolve()
    else:
        workdir = Path(f"/tmp/oci-{image}")

    if args.platform:
        platform = args.platform
    else:
        platform = get_platform()

    with mkdir(workdir, cleanup=not args.convert) as workdir:
        convert(layout, platform, workdir, image)

        if args.convert:
            raise SystemExit(f"Saved at {workdir}")

        with (
            mkdir("/run/systemd/oci-registry", cleanup=True),
            mkfile("/run/systemd/oci-registry/registry.local.oci-registry") as registry,
        ):
            registry.write_text(
                json.dumps(
                    {
                        "defaultProtocol": "file",
                        "overrideRegistry": str(workdir),
                    },
                    indent=4,
                )
                + "\n"
            )

            subprocess.run(
                [
                    "importctl",
                    "pull-oci",
                    "--class=machine",
                    "--verify=no",
                    f"local/{image}:latest",
                ]
            )


if __name__ == "__main__":
    main()
