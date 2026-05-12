#!/usr/bin/env python3
"""
Consulta em lote no Portal TUSS (POST /tuss/Pesquisar).

Lê `left_out_tuss_values.csv` no diretório do projeto (ou `--input`) e grava
um relatório com campos extraídos do primeiro resultado (e metadados de erro / total).

  venv/bin/python portaltuss_lookup_batch.py --dry-run --limit 3
  venv/bin/python portaltuss_lookup_batch.py --resume --no-async

Por omissão: httpx + asyncio, ~0,22 s entre *inícios* de pedidos (global) e
20 pedidos em voo no máximo. Use `--polite` para ~1 pedido/s e menos workers.

`--resume`: reabre `--out` e ignora códigos já presentes (retoma após falha).

`--rps N`: ignora `--sleep` e usa intervalo mínimo 1/N s entre inícios.

Atenção: taxas altas podem gerar 429 ou bloqueio; respeite os Termos de Uso.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import httpx

# Project root: directory that contains `lib/`, whether this file lives at repo
# root or under `scripts/`.
_here = Path(__file__).resolve().parent
ROOT = _here if (_here / "lib").is_dir() else _here.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.portaltuss_client import PortalTussClient


class PoliteAction(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None) -> None:
        namespace.sleep = 1.0
        namespace.workers = 6
        namespace.jitter = 0.15


class FastAction(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None) -> None:
        namespace.sleep = 0.06
        namespace.workers = 40
        namespace.jitter = 0.03


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


class AsyncRequestPacer:
    """Same as GlobalRequestPacer for asyncio (single-process)."""

    __slots__ = ("_base", "_jitter", "_lock", "_next_start")

    def __init__(self, base_interval_s: float, jitter_s: float) -> None:
        self._base = max(0.0, base_interval_s)
        self._jitter = max(0.0, jitter_s)
        self._lock = asyncio.Lock()
        self._next_start = 0.0

    async def before_request(self) -> None:
        async with self._lock:
            now = time.monotonic()
            start_at = max(now, self._next_start)
            if start_at > now:
                await asyncio.sleep(start_at - now)
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


async def run_async_fetch_batches(
    to_run: list[str],
    tuss: PortalTussClient,
    *,
    min_interval_s: float,
    jitter_s: float,
    workers: int,
    retries: int,
    chunk_size: int,
    on_chunk: Callable[[list[str], list[Any]], None],
) -> None:
    """Fetch in chunks so `on_chunk` can flush CSV rows without waiting for the full run."""
    pacer = AsyncRequestPacer(min_interval_s, jitter_s)
    sem = asyncio.Semaphore(max(1, workers))
    timeout = httpx.Timeout(tuss.timeout_s)
    lim = max(1, workers) + 8
    limits = httpx.Limits(max_connections=lim, max_keepalive_connections=lim)

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as http:

        async def one(codigo: str):
            await pacer.before_request()
            async with sem:
                return await tuss.pesquisar_codigo_async(http, codigo, max_tentativas=retries)

        cs = max(1, chunk_size)
        for start in range(0, len(to_run), cs):
            chunk = to_run[start : start + cs]
            results = await asyncio.gather(*(one(c) for c in chunk))
            on_chunk(chunk, results)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, default=ROOT / "left_out_tuss_values.csv")
    ap.add_argument("--col", type=str, default="tuss_all_codes")
    ap.add_argument("--out", type=Path, default=ROOT / "left_out_portaltuss_report.csv")
    ap.add_argument("--limit", type=int, default=0, help="0 = todos")
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.22,
        help="espaçamento mínimo global entre inícios de pedidos (s); omitido se --rps",
    )
    ap.add_argument(
        "--rps",
        type=float,
        default=0.0,
        metavar="N",
        help="se > 0, intervalo mínimo = 1/N s entre inícios (substitui --sleep)",
    )
    ap.add_argument("--jitter", type=float, default=0.05, help="±aleatório em s (0=desliga)")
    ap.add_argument(
        "--workers",
        type=int,
        default=20,
        metavar="N",
        help="máximo de pedidos HTTP em voo (async) ou threads (modo --no-async)",
    )
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=50,
        metavar="N",
        help="modo async: grava o CSV a cada N códigos (o ficheiro deixa de ficar vazio até ao fim)",
    )
    ap.add_argument("--timeout", type=float, default=30.0, help="timeout HTTP por pedido (s)")
    ap.add_argument(
        "--async",
        dest="use_async",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="httpx+asyncio (omissão); --no-async usa threads+urllib",
    )
    ap.add_argument("--polite", action=PoliteAction, help="≈1 início/s, 6 workers (portal-friendly)")
    ap.add_argument("--fast", action=FastAction, help="agressivo: ~16 inícios/s, 40 workers (risco 429)")
    ap.add_argument("--resume", action="store_true", help="continuar a partir do CSV de saída")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--base-url", type=str, default="https://portaltuss.com.br")
    args = ap.parse_args()

    min_interval = (1.0 / args.rps) if args.rps and args.rps > 0 else max(0.0, args.sleep)

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
        print(f"Resume: {len(done)} já gravados; a processar mais {len(to_run)}.", flush=True)
    elif not to_run:
        print("Nada a processar (0 códigos após filtros).", flush=True)
    else:
        print(f"A processar {len(to_run)} códigos…", flush=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if (args.resume and file_exists and done) else "w"
    workers = max(1, args.workers)
    client = PortalTussClient(base_url=args.base_url, timeout_s=args.timeout)

    n_ok = 0
    n_hit = 0
    t0 = time.monotonic()
    with args.out.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if mode == "w":
            w.writeheader()
            f.flush()
        if args.dry_run:
            for i, codigo in enumerate(to_run):
                w.writerow(
                    {k: (codigo if k == "codigo_consulta" else ("dry-run" if k == "erro" else "")) for k in cols}
                    | {"ok": "false"}
                )
        elif args.use_async:
            i_global = 0

            def on_chunk(chunk: list[str], results: list[Any]) -> None:
                nonlocal n_ok, n_hit, i_global
                for codigo, r in zip(chunk, results):
                    w.writerow(row_from_result(codigo, r))
                    if r.ok and r.error is None:
                        n_ok += 1
                        if r.total > 0:
                            n_hit += 1
                    i_global += 1
                    if i_global % 50 == 0:
                        elapsed = time.monotonic() - t0
                        print(f"  … {i_global}/{len(to_run)}  ({elapsed:.0f}s)  hits={n_hit}", flush=True)
                f.flush()

            asyncio.run(
                run_async_fetch_batches(
                    to_run,
                    client,
                    min_interval_s=min_interval,
                    jitter_s=args.jitter,
                    workers=workers,
                    retries=args.retries,
                    chunk_size=max(1, args.chunk_size),
                    on_chunk=on_chunk,
                )
            )
        else:
            pacer = GlobalRequestPacer(min_interval, args.jitter)

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
                        print(f"  … {i + 1}/{len(to_run)}  ({elapsed:.0f}s)  hits={n_hit}", flush=True)

    print(f"Feito: {len(to_run)} consultas  →  {args.out}")
    if not args.dry_run and to_run:
        mode_s = "async+httpx" if args.use_async else "threads+urllib"
        print(
            f"  pedidos OK (sem erro transporte): {n_ok}  |  com resultados (total>0): {n_hit}  |  {mode_s}"
        )
        print(
            f"  pacing: min_interval≈{min_interval:.3f}s"
            + (f"  (--rps {args.rps:g})" if args.rps and args.rps > 0 else "")
            + f"  workers={workers}"
        )
    if args.dry_run:
        print("(dry-run)")


if __name__ == "__main__":
    main()
