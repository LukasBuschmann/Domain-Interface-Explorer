# Domain Interface Explorer

Domain Interface Explorer (DIE) is a browser-based tool for inspecting protein domain-domain interfaces in aligned sequence context. It lets you compare partner-specific interaction patterns, view conservation across aligned columns, cluster similar interfaces, and open linked structure previews for selected interfaces.

## Requirements

- Conda or Mamba
- Rust (Cargo)

Quick Rust install:

```bash
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
```

## Installation

Create the Conda environment from the environment file in this repository:

```bash
conda env create -f environment.yml
conda activate domain_interface_explorer
```

## Running the Server

From the repository root:

```bash
python -m domain_interface_explorer.server
```

Then open:

```text
http://127.0.0.1:8000
```

By default, DIE uses:

- `./data` for interface JSON input
- `./cache` for generated caches
- the active Conda environment's `bin/pymol` if a Conda environment is active
- otherwise `./.conda_env/bin/pymol`

The bundled sample dataset contains 5 interface files totaling about 10 MB, so the default startup command works out of the box.

## CLI Options

Run `python -m domain_interface_explorer.server --help` to see the live help text.

Current options:

- `--host HOST`
  Default: `127.0.0.1`
  The bind address for the local web server.

- `--port PORT`
  Default: `8000`
  The TCP port for the local web server.

- `--interface-dir INTERFACE_DIR`
  Default: `./data`
  Directory containing interface JSON files.

- `--cache-dir CACHE_DIR`
  Default: `./cache`
  Directory where DIE stores selector stats, embeddings, clustering results, AlphaFold downloads, aligned models, and rendered images.

- `--pymol-bin PYMOL_BIN`
  Default: active Conda environment `bin/pymol` when detectable, otherwise `./.conda_env/bin/pymol`
  PyMOL executable used for alignment-based structure outputs.

Example with custom paths:

```bash
python -m domain_interface_explorer.server \
  --host 0.0.0.0 \
  --port 8080 \
  --interface-dir /path/to/interface-json-dir \
  --cache-dir /path/to/cache \
  --pymol-bin /path/to/pymol
```

## Quick Feature Tour

- Interface picker: Choose a domain-domain interface dataset from the loaded JSON files.
- Partner filter: Restrict the view to interfaces against one partner domain or inspect all partners together.
- Alignment view: Browse interfaces in aligned sequence context with conservation and interface/surface overlays.
- Search: Fuzzy-search visible interfaces by label.
- Embeddings: Compute a 3D t-SNE embedding of interface similarity.
- Clustering: Group similar interfaces with hierarchical clustering or HDBSCAN.
- Distance matrix view: Inspect pairwise interface similarity directly.
- Column view: Explore how interface signal is distributed across alignment columns.
- Structure preview: Open an interactive 3D view for a selected interface using bundled 3Dmol.js.
- AlphaFold integration: Fetch models on demand and cache them locally for later reuse.
- PyMOL alignment support: If PyMOL is installed, DIE can generate aligned structure outputs for comparative viewing.

## Operational Notes

- The first run may build the Rust helper automatically if `cargo` is installed and the binary is missing.
- The first structure request for a protein may download AlphaFold data from EBI.
- Cache files are safe to delete if you want DIE to recompute them.
- Start the server with `python -m domain_interface_explorer.server`, not by running `server.py` directly.

## GitHub Actions Builds

The repository includes a workflow at `.github/workflows/build-interface-distance.yml` that builds the
`interface_distance` Rust helper on GitHub-hosted Linux, Windows, and macOS runners.

To try it on a branch:

```bash
git push origin <branch-name>
```

You can also trigger it manually from the Actions tab with `workflow_dispatch`.

Each run uploads one artifact per platform:

- `linux-x86_64`
- `windows-x86_64`
- `macos-x86_64`
- `macos-arm64`

The artifacts are `.tar.gz` archives containing the built `interface_distance` binary for that platform.
