"""Build an API-only probe against an unmodified, pinned official Codex checkout."""
import argparse
import json
import pathlib
import subprocess
import shutil
import tomllib
ROOT = pathlib.Path(__file__).resolve().parents[1]
PIN = 'f07aaf920b14d7a746e435add34b8bbd37da5da6'
parser = argparse.ArgumentParser()
parser.add_argument('--codex-source', required=True, type=pathlib.Path)
args = parser.parse_args()
source = args.codex_source.resolve()
assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip() == PIN
assert subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True).strip() == '', 'Codex checkout must be unmodified'
build = ROOT/'target'/'codex-sdk-probe'
build.mkdir(parents=True, exist_ok=True)
manifest = f'''[package]
name = "cpa-codex-sdk-probe"
version = "0.0.0"
edition = "2024"
[workspace]
[[bin]]
name = "cpa-codex-sdk-probe"
path = {json.dumps(str(ROOT/'scripts'/'codex_sdk_probe.rs'))}
[dependencies]
codex-api = {{path={json.dumps(str(source/'codex-rs'/'codex-api'))}}}
futures = "0.3"
http = "1"
reqwest = "0.12"
serde_json = "1"
tokio = {{version="1",features=["macros","rt-multi-thread"]}}
[profile.dev]
debug = 0
'''
# Preserve the official SDK's dependency patches; do not alter SDK sources.
patches = tomllib.loads((source/'codex-rs'/'Cargo.toml').read_text())['patch']
for registry, crates in patches.items():
    manifest += '\n[patch.' + json.dumps(registry) + ']\n'
    for name, spec in crates.items():
        manifest += name+' = {'+', '.join(key+' = '+json.dumps(value) for key,value in spec.items())+'}\n'
(build/'Cargo.toml').write_text(manifest)
# Reuse Codex's tested transitive versions (not newly released incompatible dependencies).
shutil.copyfile(source/'codex-rs'/'Cargo.lock', build/'Cargo.lock')
subprocess.run(['cargo','build','--manifest-path',str(build/'Cargo.toml')],check=True)
print(build/'target'/'debug'/'cpa-codex-sdk-probe')
