"""Package a native build into CPA's platform directory layout."""
import hashlib
import pathlib
import platform
import tomllib
import zipfile

root = pathlib.Path(__file__).resolve().parents[1]
version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
goos = {"Linux": "linux", "Darwin": "darwin", "Windows": "windows"}[platform.system()]
goarch = {"x86_64": "amd64", "AMD64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[platform.machine()]
extension = {"linux": ".so", "darwin": ".dylib", "windows": ".dll"}[goos]
library = ("" if goos == "windows" else "lib") + "cpa_stg" + extension
source = root / "target" / "release" / library
if not source.is_file():
    raise SystemExit(f"Build first with cargo build --workspace --release --locked: missing {source}")
dist = root / "dist"
dist.mkdir(exist_ok=True)
archive = dist / f"cpa-stg_{version}_{goos}_{goarch}.zip"
with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
    bundle.write(source, f"{goos}/{goarch}/cpa-stg{extension}")
    router = source.with_name(("" if goos == "windows" else "lib") + "cpa_stg_router" + extension)
    if not router.is_file():
        raise SystemExit("Build both components with cargo build --workspace --release --locked")
    bundle.write(router, f"{goos}/{goarch}/cpa-stg-router{extension}")
    for name in ["README.md", "README_CN.md", "TESTING.md", "config.example.yaml", "LICENSE"]:
        bundle.write(root / name, name)
    for evidence in sorted((root / "test-results").glob("*")):
        if evidence.is_file():
            bundle.write(evidence, "test-results/" + evidence.name)
checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
(dist / "checksums.txt").write_text(f"{checksum}  {archive.name}\n")
print(archive)
