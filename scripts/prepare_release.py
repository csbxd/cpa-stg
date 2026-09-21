"""Validate CI artifacts and prepare assets for a GitHub release (no rebuild)."""
import hashlib
import json
import os
import pathlib
import shutil
import tomllib
import zipfile


root = pathlib.Path(__file__).resolve().parents[1]
version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
tag = "v" + version
if os.environ.get("GITHUB_REF_TYPE") == "tag" and os.environ["GITHUB_REF_NAME"] != tag:
    raise SystemExit("Release tag does not match Cargo.toml")

artifacts = root / "target/release-artifacts"
archives = sorted(artifacts.glob("*/*.zip"))
if len(archives) != 3:
    raise SystemExit(f"Expected all three platform packages, got {archives}")
dist = root / "dist"
shutil.rmtree(dist, ignore_errors=True)
dist.mkdir()
platforms = set()
for archive in archives:
    checksums = (archive.parent / "checksums.txt").read_text().splitlines()
    expected = [line.split()[0] for line in checksums if line.split()[-1] == archive.name]
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if expected != [digest]:
        raise SystemExit(f"Checksum mismatch: {archive.name}")
    prefix = f"cpa-stg_{version}_"
    if not archive.name.startswith(prefix):
        raise SystemExit(f"Unexpected package version: {archive.name}")
    platform = archive.name.removeprefix(prefix).removesuffix(".zip")
    if platform in platforms:
        raise SystemExit(f"Duplicate platform: {platform}")
    platforms.add(platform)
    shutil.copy2(archive, dist / archive.name)
    if platform == "linux_amd64":
        with zipfile.ZipFile(archive) as bundle:
            for name in ["cpa-stg", "cpa-stg-router"]:
                data = bundle.read(f"linux/amd64/{name}.so")
                if not data.startswith(b"\x7fELF"):
                    raise SystemExit(f"Not an ELF library: {name}")
                (dist / f"{name}_{version}_linux_amd64.so").write_bytes(data)
if "linux_amd64" not in platforms:
    raise SystemExit("Missing Linux amd64 package")

report = root / "target/release-evidence/report.json"
results = json.loads(report.read_text())
checks = results["results"] if isinstance(results, dict) else results
if len(checks) < 16 or not all(check.get("passed") is True for check in checks):
    raise SystemExit("Incomplete or unsuccessful CPA end-to-end evidence")
shutil.copy2(report, dist / "e2e-report.json")
commit = os.environ["GITHUB_SHA"]
run_url = (f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}"
           f"/actions/runs/{os.environ['GITHUB_RUN_ID']}")
(dist / "build-info.json").write_text(json.dumps({
    "version": version, "commit": commit, "workflow_run": run_url,
    "platforms": sorted(platforms), "linux_e2e_checks": len(checks),
}, indent=2) + "\n")
(dist / "checksums.txt").write_text("".join(
    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
    for path in sorted(dist.iterdir()) if path.name != "checksums.txt"
))
notes = (root / "RELEASE_NOTES.md").read_text()
(root / "target/release-notes.md").write_text(
    notes + f"\nCommit: `{commit}`\n\nBuilt and tested by [GitHub Actions]({run_url}).\n")
with open(os.environ["GITHUB_OUTPUT"], "a") as output:
    output.write(f"tag={tag}\n")
print(f"Prepared {tag}: {len(list(dist.iterdir()))} verified release assets")
