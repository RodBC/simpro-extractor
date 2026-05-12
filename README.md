# simpro-extractor

Tools for pulling structured data from the [Portal TUSS](https://portaltuss.com.br) search API (`POST /tuss/Pesquisar`). The main entry point is a batch job that reads TUSS codes from a CSV, looks up each code, and writes a report CSV with the first hit, HTTP metadata, and extra fields (ANVISA code, manufacturer, validity dates, category flags, and similar).

## What it does

- **Batch lookup** (`portaltuss_lookup_batch.py`): loads codes from an input column (default `tuss_all_codes`), calls the portal for each code with configurable delay, jitter, and retries, and appends rows to an output CSV.
- **Resume**: `--resume` skips codes already present in the output file so a long run can continue after interruption.
- **Dry run**: `--dry-run` writes placeholder rows without calling the network (useful to validate CSV paths and columns).

Use delays and stay within the portal’s terms of use; large batches (for example thousands of codes at one request per second) take a long time and load the remote service.

## Requirements

- Python **3.10+** (the script uses modern type syntax).
- Dependencies: see `requirements.txt` (install with `pip install -r requirements.txt`).

## Setup

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

The batch script imports `PortalTussClient` from `lib.portaltuss_client`. Ensure a `lib/` package with that module exists on `PYTHONPATH` (typically alongside the project root used as `ROOT` in the script).

## Usage (examples)

```bash
python portaltuss_lookup_batch.py --dry-run --limit 3
python portaltuss_lookup_batch.py --resume --sleep 1.0
```

Useful flags include `--input`, `--out`, `--col`, `--limit`, `--sleep`, `--jitter`, `--retries`, `--base-url`, and `--resume`. Defaults assume a project layout where the repository root is on `sys.path` and data files live under `data/`; override `--input` and `--out` if your files live elsewhere.

## Repository layout (intended)

- `portaltuss_lookup_batch.py` — batch driver and CSV I/O.
- `lib/portaltuss_client.py` — HTTP client for the portal (not necessarily committed here; add it locally if missing).
- `data/` — optional default location for input and report CSVs.

## License / compliance

Respect Portal TUSS terms of use and applicable regulations when automating lookups. This repository is for data extraction and reporting workflows tied to TUSS/SimPRO-style operational needs; adjust paths and credentials to match your environment.
