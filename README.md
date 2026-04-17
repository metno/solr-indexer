# solrindexer

Tools and wrappers for indexing [MMD](https://github.com/metno/mmd) metadata into Apache Solr.
Developed in the context of the Arctic Data Centre, supported through the SIOS-KC and Norwegian
Scientific Data Network projects.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [SKOS vocabulary validation](#skos-vocabulary-validation)
- [Commands](#commands)
  - [indexdata](#indexdata)
  - [searchindex](#searchindex)
- [Logging](#logging)
- [Development](#development)

---

## Installation

### Prerequisites

- Python 3.9 or later
- A running Apache Solr instance with the [metsis-solr-configsets](https://github.com/metno/metsis-solr-configsets) applied

### Option A: Install as a package (recommended)

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install the package and its dependencies:

```bash
pip install .
```

This adds `indexdata` and `searchindex` to your `PATH`.

Optional extras:

```bash
# For vocabulary validation using rdflib
pip install ".[rdflib]"

# For rich terminal output (colours, syntax highlighting)
pip install ".[rich]"

# For the legacy metvocab vocabulary backend
pip install ".[metvocab]"

# Install all development tools (pytest, mypy, ruff, bandit, …)
pip install ".[dev]"
```

### Option B: Run without installing (standalone)

If dependencies are installed in the active Python environment, you can run the
root-level wrapper scripts directly without installing the package:

```bash
./indexdata -c etc/config.yml -d /data/mmd
./searchindex -c etc/config.yml -s "metadata_identifier:*"
```

### Install using Conda

```bash
conda env create -f environment.yml
conda activate solrindexing
pip install .
```

### Install on PPI

```bash
source /modules/rhel8/conda/install/etc/profile.d/conda.sh
conda activate production-08-2024
pip install .
```

---

## Configuration

All commands require a YAML configuration file passed with `-c`. A fully annotated template is
provided at [`etc/cfg-template.yml`](etc/cfg-template.yml).

### Minimal configuration

```yaml
solrserver: http://localhost:8983/solr/
solrcore: mmd-data
```

### Authentication

Credentials can be supplied inline or via a `.env` file (preferred to avoid storing passwords in
config files):

```yaml
# Inline (less secure)
auth-basic-username: solr_user
auth-basic-password: secret

# .env file (SOLR_USERNAME / SOLR_PASSWORD must be set inside the file)
dotenv_path: /etc/solrindexer/.env
```

If neither `auth-basic-username` nor `dotenv_path` is present the command looks for a `.env`
file in the current working directory. If `SOLR_USERNAME` and `SOLR_PASSWORD` are empty,
authentication is disabled.

### Configuration reference

| Key | Default | Description |
|-----|---------|-------------|
| `solrserver` | — | **Required.** Base URL of the Solr instance, e.g. `http://localhost:8983/solr/` |
| `solrcore` | — | **Required.** Solr core or collection name |
| `auth-basic-username` | — | Solr basic-auth username |
| `auth-basic-password` | — | Solr basic-auth password |
| `dotenv_path` | — | Absolute path to a `.env` file containing `SOLR_USERNAME` / `SOLR_PASSWORD` |
| `batch-size` | `2500` | Documents sent to Solr per HTTP request |
| `workers` | `1` | Parallel OS-level worker processes (for very large datasets) |
| `threads` | `20` | Worker threads per process for I/O-bound indexing |
| `end-solr-commit` | `false` | Send a hard commit to Solr when indexing finishes |
| `skip-feature-type` | `false` | Skip OPeNDAP feature-type extraction |
| `override-feature-type` | — | Force a specific feature type for all documents (also skips extraction) |
| `mmd-xsd-path` | — | Path to `mmd.xsd` for optional XSD validation (warns; never blocks indexing) |
| `vocabulary-backend` | `native` | Vocabulary backend: `native`, `legacy-metvocab`, or `rest-skosmos` |
| `vocabulary-ttl-path` | — | Path to a local MMD vocabulary TTL file (used with `native` backend) |
| `vocabulary-endpoint-base-url` | `https://vocab.met.no/mmd` | Base URL for REST Skosmos vocabulary lookups |
| `vocabulary-endpoint-timeout` | `20.0` | HTTP timeout (seconds) for REST vocabulary requests |
| `vocabulary-cache-dir` | system temp (`/tmp/...`) | Directory for persisted REST vocabulary cache files; set to shared storage on multi-node clusters |
| `nbs-thumbnails-base-path` | — | Filesystem root where NBS `thumbnail.png` files are stored |
| `nbs-thumbnails-base-url` | — | Public base URL from which NBS thumbnails are served |
| `scope` | — | Set to `NBS` to enable NBS-specific thumbnail lookup |

See [`etc/cfg-template.yml`](etc/cfg-template.yml) for a fully commented example.

### SKOS vocabulary validation

The indexer validatescontrolled vocabulary fields against MMD SKOS vocabularies.
You can validate by either:

- using a local TTL file (`vocabulary-backend: native` + `vocabulary-ttl-path`), or
- using the REST SKOS endpoint (`vocabulary-backend: rest-skosmos`).

The MET Norway vocabulary service is available at
[https://vocab.met.no](https://vocab.met.no).

For MMD vocabularies, use `vocabulary-endpoint-base-url: https://vocab.met.no/mmd`.

### XSD Validation and Schema Caching

When XSD validation is enabled (via `mmd-xsd-path`), the indexer caches compiled schemas
in thread-local storage to eliminate per-document disk I/O and compilation overhead.
This optimization is automatic and transparent:

- **Single-process indexing**: Each of the worker threads maintains its own schema cache
- **Multi-process indexing**: Each worker process pre-compiles the schema once before indexing begins
- **Scope**: Schemas are cached for the entire indexing run; no live reloads

This approach ensures optimal performance for large bulk indexing jobs while maintaining
thread-safe concurrent validation (lxml XMLSchema validation is not thread-safe; thread-local
caching prevents race conditions).

---

## Commands

### indexdata

Indexes one or more MMD XML files into Solr. Supports single files, file lists, and directories,
with optional multi-process and multi-thread parallelism for large-scale ingestion.

```text
usage: indexdata -c CFGFILE (-i FILE | -l LIST | -d DIR | -parent ID) [options]

required:
  -c, --cfg CFGFILE         Path to YAML configuration file

input (one required):
  -i, --input_file FILE     Index a single MMD XML file
  -l, --list_file LIST      Index files listed one per line in FILE
  -d, --directory DIR       Index all XML files in DIR (non-recursive by default)
  -parent, --mark_parent ID Mark an existing Solr document as a parent dataset

input modifiers:
  -r, --recursive           Recurse into subdirectories when using -d

performance:
  --threads N               Worker threads per process (default: config or 20)
  --processes N             Parallel OS-level processes for large bulk runs (default: 1)
  --chunksize N             Documents per Solr HTTP request (default: config or 2500)

solr:
  -a, --always_commit       Commit to Solr after every batch

thumbnails (NBS scope only):
  -t, --thumbnail           Enable NBS thumbnail URL lookup
  -n, --no_thumbnail        Disable thumbnail lookup
  -nbs, --nbs               Enable NBS scope (also activates thumbnail lookup)

exit codes:
  0                         Completed with no tracked failures or warnings
  1                         Completed with one or more tracked failures, or aborted on runtime/config error
  2                         CLI usage error (for example, missing required input arguments)
  3                         Completed with warnings only and no tracked failures
```

#### Examples

Index a single file:

```bash
indexdata -c etc/config.yml -i tests/data/full_mmdv4.xml
```

Index all XML files in a directory (non-recursive):

```bash
indexdata -c etc/config.yml -d /data/mmd
```

Index an entire directory tree recursively with 4 threads:

```bash
indexdata -c etc/config.yml -d /data/mmd -r --threads 4
```

Index a large dataset using 2 parallel processes, 8 threads each, batch size 1000:

```bash
indexdata -c etc/config.yml -d /data/mmd -r --processes 2 --threads 8 --chunksize 1000
```

Index from a file list:

```bash
find /data/mmd -name "*.xml" > filelist.txt
indexdata -c etc/config.yml -l filelist.txt
```

Mark an existing document as a parent:

```bash
indexdata -c etc/config.yml -parent "no.met:c5ccf1dc-b223-4f44-984b-7edf95ba6012"
```

---

### searchindex

Searches the Solr index for MMD documents. Supports full Solr query syntax, optional deletion of
matched records, and returning raw MMD XML.

```text
usage: searchindex -c CFGFILE -s QUERY [options]

required:
  -c, --cfg CFGFILE         Path to YAML configuration file
  -s, --searchstringst QUERY
                            Solr query string (full Lucene/Solr syntax supported)

options:
  -d, --delete              Delete all documents matching the query
  -a, --always_commit       Commit immediately after deletion
  --mmd                     Return raw MMD XML (mmd_xml_file field) instead of JSON
```

#### Examples

Search by metadata identifier:

```bash
searchindex -c etc/config.yml -s "metadata_identifier:no.met:c5ccf1dc-b223-4f44-984b-7edf95ba6012"
```

Free-text search:

```bash
searchindex -c etc/config.yml -s "sea ice"
```

Search and delete matched records:

```bash
searchindex -c etc/config.yml -s "collection:METNCS" -d
```

Retrieve raw MMD XML for a document:

```bash
searchindex -c etc/config.yml -s "metadata_identifier:no.met:abc123" --mmd
```

> **Note:** When no field is specified, Solr searches the default `full_text` field.

---

## Logging

Logging is controlled via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SOLRINDEXER_LOGLEVEL` | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `SOLRINDEXER_LOGFILE` | — | If set, log output is also written to this file path |

Examples:

```bash
# Verbose debug output to console
SOLRINDEXER_LOGLEVEL=DEBUG indexdata -c etc/config.yml -i file.xml

# Separate info and error streams
indexdata -c etc/config.yml -d /data/mmd > index.log 2> errors.log

# Log to file in addition to console
SOLRINDEXER_LOGFILE=/var/log/solrindexer.log indexdata -c etc/config.yml -d /data/mmd
```

---

## Development

### Running tests

```bash
# Run the full test suite
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_indexdata.py -v

# Run only tests matching a marker
python -m pytest -m indexdata -v
```

### Using tox (recommended)

All quality checks are managed through [tox](https://tox.wiki):

```bash
tox -e py3          # Full test suite
tox -e lint         # Lint with ruff
tox -e lint-fix     # Auto-fix lint warnings
tox -e format-check # Check formatting
tox -e format       # Auto-format code
tox -e typecheck    # Type checking with mypy
tox -e security     # Security scanning with bandit
tox -e prospector   # Prospector code analysis
tox                 # Run all environments
```

### Project structure

```text
solr-indexer/
├── solrindexer/
│   ├── __init__.py        # Package init; exports IndexMMD, MMD4SolR, BulkIndexer
│   ├── cli.py             # indexdata CLI entry point
│   ├── search.py          # searchindex CLI entry point
│   ├── mmd.py             # MMD4SolR (XML→Solr doc) and IndexMMD (Solr connection)
│   ├── indexer.py         # BulkIndexer (multi-threaded bulk ingestion)
│   ├── spatial.py         # Geospatial helpers (WKT, GeoJSON, Solr envelopes)
│   ├── tools.py           # Utility functions (date parsing, Solr helpers, thumbnails)
│   ├── threads.py         # Concurrent processing helpers
│   ├── io.py              # File loading helpers
│   ├── vocabulary.py      # Vocabulary validation backends
│   ├── failure_tracker.py # Per-run failure and warning tracking
│   └── xmlutils.py        # XML parsing utilities
├── tests/                 # pytest test suite
├── etc/
│   └── cfg-template.yml   # Annotated configuration template
├── indexdata              # Standalone wrapper script (no install needed)
├── searchindex            # Standalone wrapper script (no install needed)
├── pyproject.toml
└── tox.ini
```
