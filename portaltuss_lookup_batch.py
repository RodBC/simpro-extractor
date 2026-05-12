#!/usr/bin/env python3
"""
Consulta em lote no Portal TUSS (POST /tuss/Pesquisar).

Lê `left_out_tuss_values.csv` no diretório do projeto (ou `--input`) e grava
um relatório com campos extraídos do primeiro resultado (e metadados de erro / total).

  venv/bin/python portaltuss_lookup_batch.py --dry-run --limit 3
  venv/bin/python portaltuss_lookup_batch.py --resume --sleep 1.0 --workers 6

`--resume`: reabre `--out` e ignora códigos já presentes (retoma após falha).

`--workers`: pedidos HTTP em paralelo (I/O). `--sleep` é o espaçamento mínimo
global entre *inícios* de pedidos (com `--jitter`), evitando rajadas.

Atenção: ~2500 pedidos × 1 s ≈ 40 min; respeite os Termos de Uso do portal.
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Project root: directory that contains `lib/`, whether this file lives at repo
# root or under `scripts/`.
_here = Path(__file__).resolve().parent
ROOT = _here if (_here / "lib").is_dir() else _here.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.portaltuss_client import PortalTussClient


class GlobalRequestPacer:
    """Minimum spacing between the *start* of each HTTP request (all threads)."""

    __slots__ = ("_base", "_jitter", "_lock", "_next_start")

    def __init__(self, base_interval_s: float, jitter_s: float) -> None:
        self._base = max(0.0, base_interval_s)
        self._jitter = max(0.0, jitter_s)
        self._lock = threading.Lock()
        self._next_start = 0.0

    def before_request(self) -> None:
        with self._lock:
            now = time.monotonic()
            start_at = max(now, self._next_start)
            if start_at > now:
                time.sleep(start_at - now)
                now = time.monotonic()
            jitter = random.uniform(-self._jitter, self._jitter) if self._jitter > 0 else 0.0
            interval = max(0.0, self._base + jitter)
            self._next_start = now + interval


_EXTRA_KEYS = (
    "codigo_anvisa",
    "complemento",
    "fabricante",
    "descricao_detalhada",
    "nome_tecnico",
    "data_inicio_vigencia",
    "data_fim_vigencia",
    "data_implantacao",
    "dia_mes_ano_fim_vigencia",
    "eh_medicamento",
    "eh_procedimento",
    "eh_material",
    "eh_diarias_taxas",
    "state",
    "categoria_key",
)


def load_codes(path: Path, col: str | None) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        fn = dr.fieldnames or []
        c = col if col and col in fn else fn[0]
        return [(row.get(c) or "").strip() for row in dr if (row.get(c) or "").strip()]


def load_done_codigos(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    done: set[str] = set()
    with path.open(newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        if not dr.fieldnames or "codigo_consulta" not in dr.fieldnames:
            return set()
        for row in dr:
            v = (row.get("codigo_consulta") or "").strip()
            if v:
                done.add(v)
    return done


def fieldnames() -> list[str]:
    base = [
        "codigo_consulta",
        "ok",
        "http_status",
        "total_resultados",
        "match_codigo_exato",
        "codigo_tuss_retorno",
        "descricao",
        "vigencia",
        "categoria",
        "erro",
    ]
    return base + list(_EXTRA_KEYS)


def row_from_result(codigo: str, r) -> dict[str, object]:
    ret = r.first_codigo or ""
    match_exato = ""
    if r.first_codigo is not None:
        match_exato = str(codigo == (r.first_codigo or "")).lower()
    row: dict[str, object] = {
        "codigo_consulta": codigo,
        "ok": r.ok and r.error is None,
        "http_status": r.http_status if r.http_status is not None else "",
        "total_resultados": r.total,
        "match_codigo_exato": match_exato,
        "codigo_tuss_retorno": ret,
        "descricao": (r.first_descricao or "")[:4000],
        "vigencia": r.first_vigencia or "",
        "categoria": r.first_categoria or "",
        "erro": r.error or "",
    }
    ex = r.extras or {}
    for k in _EXTRA_KEYS:
        row[k] = ex.get(k, "")
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, default=ROOT / "left_out_tuss_values.csv")
    ap.add_argument("--col", type=str, default="tuss_all_codes")
    ap.add_argument("--out", type=Path, default=ROOT / "left_out_portaltuss_report.csv")
    ap.add_argument("--limit", type=int, default=0, help="0 = todos")
    ap.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="espaçamento mínimo global entre inícios de pedidos (s); com vários workers, vários RTTs podem sobrepor-se",
    )
    ap.add_argument("--jitter", type=float, default=0.15, help="±aleatório em s (0=desliga)")
    ap.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="threads para pedidos HTTP em paralelo (1 = só uma em voo; use valores modestos)",
    )
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--resume", action="store_true", help="continuar a partir do CSV de saída")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--base-url", type=str, default="https://portaltuss.com.br")
    args = ap.parse_args()

    cols = fieldnames()
    codes = load_codes(args.input, args.col)
    if args.limit > 0:
        codes = codes[: args.limit]

    done: set[str] = set()
    file_exists = args.out.exists() and args.out.stat().st_size > 0
    if args.resume and file_exists:
        done = load_done_codigos(args.out)

    to_run = [c for c in codes if c not in done]
    if args.resume and file_exists:
        print(f"Resume: {len(done)} já gravados; a processar mais {len(to_run)}.")
    args.out.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if (args.resume and file_exists and done) else "w"
    workers = max(1, args.workers)
    client = PortalTussClient(base_url=args.base_url)
    pacer = GlobalRequestPacer(args.sleep, args.jitter)

    n_ok = 0
    n_hit = 0
    t0 = time.monotonic()
    with args.out.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if mode == "w":
            w.writeheader()
        if args.dry_run:
            for i, codigo in enumerate(to_run):
                w.writerow(
                    {k: (codigo if k == "codigo_consulta" else ("dry-run" if k == "erro" else "")) for k in cols}
                    | {"ok": "false"}
                )
        else:

            def fetch(codigo: str):
                pacer.before_request()
                return client.pesquisar_codigo(codigo, delay_s=0.0, max_tentativas=args.retries)

            with ThreadPoolExecutor(max_workers=workers) as ex:
                for i, (codigo, r) in enumerate(zip(to_run, ex.map(fetch, to_run))):
                    row = row_from_result(codigo, r)
                    w.writerow(row)
                    f.flush()
                    if r.ok and r.error is None:
                        n_ok += 1
                        if r.total > 0:
                            n_hit += 1
                    if (i + 1) % 50 == 0:
                        elapsed = time.monotonic() - t0
                        print(f"  … {i + 1}/{len(to_run)}  ({elapsed:.0f}s)  hits={n_hit}")

    print(f"Feito: {len(to_run)} consultas  →  {args.out}")
    if not args.dry_run and to_run:
        print(f"  pedidos OK (sem erro transporte): {n_ok}  |  com resultados (total>0): {n_hit}")
    if args.dry_run:
        print("(dry-run)")


if __name__ == "__main__":
    main()
