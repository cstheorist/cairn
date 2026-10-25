#!/usr/bin/env python3
"""Download pinned, hash-checked formal tools into .tools. Requires rustup separately."""
import hashlib
import pathlib
import platform
import urllib.request
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
DESTINATION = ROOT / ".tools"
VERSION = "0.2026.10.18.7d82994"
PLATFORMS = {
    ("Darwin", "arm64"): ("arm64-macos", "3fc56c58277b5f0999368f7b3a2b07ba4eb9ec2965454ce7a9f2769c0464d32b"),
    ("Darwin", "x86_64"): ("x86-macos", "503c50d82bc7588f5557eb4927ff1e2d523997d770b84267a4e5886d0d6f025f"),
    ("Linux", "x86_64"): ("x86-linux", "e9af03732d4bdd32b93a8fcf471706739c24c1473fa5086618d96c47237845dd"),
}


def download(url, filename, sha256):
    path = DESTINATION / filename
    if not path.exists():
        print(f"downloading {filename}", flush=True)
        temporary = path.with_suffix(path.suffix + ".download")
        urllib.request.urlretrieve(url, temporary)
        temporary.rename(path)
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""): hasher.update(chunk)
    if hasher.hexdigest() != sha256:
        raise SystemExit(f"SHA-256 mismatch: {path}; remove that file and investigate before retrying")
    return path


def main():
    target = PLATFORMS.get((platform.system(), platform.machine()))
    if target is None:
        raise SystemExit("No pinned Verus binary for this platform. See https://github.com/verus-lang/verus/blob/main/INSTALL.md")
    DESTINATION.mkdir(exist_ok=True)
    suffix, digest = target
    filename = f"verus-{VERSION}-{suffix}.zip"
    archive = download(f"https://github.com/verus-lang/verus/releases/download/release/{VERSION}/{filename}", filename, digest)
    binary = DESTINATION / f"verus-{suffix}" / "verus"
    if not binary.exists():
        with zipfile.ZipFile(archive) as zipped:
            for item in zipped.infolist():
                path = (DESTINATION / item.filename).resolve()
                if DESTINATION.resolve() not in path.parents:
                    raise SystemExit("archive contains an unsafe path")
                zipped.extract(item, DESTINATION)
                path.chmod((item.external_attr >> 16) & 0o777 or 0o644)
    download("https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar", "tla2tools.jar",
             "d15655eeaff4b3f7b3d65c46b838bc0f35d131b2ef4b1e473689b62c98d4726f")
    print(f"VERUS={binary}")
    print("Install the Rust toolchain requested by Verus (this release requires 1.99.1).")


if __name__ == "__main__": main()
