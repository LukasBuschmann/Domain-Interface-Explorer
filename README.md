# Domain Interface Explorer

Domain Interface Explorer (DIE) is a browser-based tool for inspecting protein domain-domain interfaces in aligned sequence context. It lets you compare partner-specific interaction patterns, view conservation across aligned columns, cluster similar interfaces, and open linked structure previews for selected interfaces.

## Getting the Repo

If you use Git, clone the repository and enter the project directory:

```bash
git clone git@github.com:LukasBuschmann/Domain-Interface-Explorer.git
cd Domain-Interface-Explorer
```

To update it later:

```bash
git pull
```

_If you do not use Git, open the repository on GitHub, choose `Code`, then `Download ZIP`. Extract the archive and open the extracted `Domain-Interface-Explorer` folder. To update later, download a fresh ZIP._

## Requirements

- Conda or Mamba
- _If you do not have Conda yet, install a minimal distribution first. Official install docs:_
  - _Windows: https://docs.conda.io/projects/conda/en/stable/user-guide/install/windows.html_
  - _macOS: https://docs.conda.io/projects/conda/en/stable/user-guide/install/macos.html_
  - _Linux: https://docs.conda.io/projects/conda/en/stable/user-guide/install/linux.html_

## Installation

Create the Conda environment from the environment file in this repository:

```bash
conda env create -f environment.yml -p .conda_env
```

## Running the Server

From the repository root:

```bash
conda activate ./.conda_env
python -m domain_interface_explorer.server
```

Then open:

```text
http://127.0.0.1:8000
```

By default, DIE uses:

- `./data` for interface JSON input (`.json` or `.json.gz`)
- `./cache` for generated caches

The bundled sample dataset contains 5 interface files totaling about 10 MB, so the default startup command works out of the box.

## Frontend Development

The editable frontend source lives in `frontend/src`. Generated browser modules are bundled in
`domain_interface_explorer/static/dist` so normal installs do not require Node.js.

If you edit the frontend, install Node.js and npm, then rebuild:

```bash
npm install
npm run build
```

GitHub Actions rebuilds the frontend on pushes that touch frontend sources or build config, then
commits updated bundled assets back to the branch.

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
  Directory containing interface JSON files (`.json` or `.json.gz`).

- `--cache-dir CACHE_DIR`
  Default: `./cache`
  Directory where DIE stores selector stats, distance matrices, embeddings, clustering results, AlphaFold downloads, aligned models, and rendered images.

- `--cache-workers CACHE_WORKERS`
  Default: `4`
  Maximum worker count for cache-building jobs, including selector stats and lazy overlap-distance caches.

Example with custom paths:

```bash
python -m domain_interface_explorer.server \
  --host 0.0.0.0 \
  --port 8080 \
  --interface-dir /path/to/interface-json-dir \
  --cache-dir /path/to/cache \
  --cache-workers 4
```

## Adding Data

To add new interface datasets, place `.json` or `.json.gz` files into the default data directory:

```text
./data
```

You can also point the server at a different directory with `--interface-dir`.

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
- Embedded PyMOL alignment support: If the `pymol-open-source` package imports successfully, DIE can generate aligned structure outputs for comparative viewing without a separate `pymol` binary.

## Operational Notes

- The first structure request for a protein may download AlphaFold data from EBI.
- Cache files are safe to delete if you want DIE to recompute them.
- Start the server with `python -m domain_interface_explorer.server`, not by running `server.py` directly.

## GitHub Actions Builds

The repository includes a workflow at `.github/workflows/build-interface-distance.yml` that builds the
`interface_distance` native C helper on GitHub-hosted Linux, Windows, and macOS runners.

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

The workflow outputs should be unpacked into these tracked paths:

- `interface_distance/bin/linux-x86_64/interface_distance`
- `interface_distance/bin/windows-x86_64/interface_distance.exe`
- `interface_distance/bin/macos-x86_64/interface_distance`
- `interface_distance/bin/macos-arm64/interface_distance`
