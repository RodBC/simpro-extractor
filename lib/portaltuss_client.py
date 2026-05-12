"""
Cliente para a pesquisa pública do Portal TUSS (Simpro).

POST /tuss/Pesquisar com JSON: {"termo": "<texto ou código>", "categorias": []}
Cabeçalhos Origin + Referer são necessários para resposta válida.

Uso responsável: limitar taxa de pedidos; respeitar os Termos de Uso do portal.
"""
from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


DEFAULT_BASE = "https://portaltuss.com.br"
PESQUISAR_PATH = "/tuss/Pesquisar"


@dataclass
class PortalTussSearchResult:
    ok: bool
    http_status: int | None
    total: int
    first_codigo: str | None
    first_descricao: str | None
    first_vigencia: str | None
    first_categoria: str | None
    raw: dict[str, Any] | None
    error: str | None = None
    extras: dict[str, str] = field(default_factory=dict)


def _flatten_first_hit(hit: dict[str, Any]) -> dict[str, str]:
    cat = hit.get("categoriaValue") or {}
    if isinstance(cat, dict):
        cat_txt = str(cat.get("text") or "")
    else:
        cat_txt = ""
    return {
        "codigo_anvisa": str(hit.get("codigoAnvisa") or ""),
        "complemento": str(hit.get("complemento") or "")[:4000],
        "fabricante": str(hit.get("fabricante") or "")[:2000],
        "descricao_detalhada": str(hit.get("descricaoDetalhada") or "")[:4000],
        "nome_tecnico": str(hit.get("nomeTecnico") or "")[:2000],
        "data_inicio_vigencia": str(hit.get("dataInicioVigencia") or ""),
        "data_fim_vigencia": str(hit.get("dataFimVigencia") or ""),
        "data_implantacao": str(hit.get("dataImplantacao") or ""),
        "dia_mes_ano_fim_vigencia": str(hit.get("diaMesAnoDataFimVigencia") or ""),
        "eh_medicamento": str(hit.get("ehMedicamento")),
        "eh_procedimento": str(hit.get("ehProcedimento")),
        "eh_material": str(hit.get("ehMaterial")),
        "eh_diarias_taxas": str(hit.get("ehDiariasTaxas")),
        "state": str(hit.get("state") or ""),
        "categoria_key": str(cat.get("key") or "") if isinstance(cat, dict) else "",
    }


class PortalTussClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE,
        *,
        timeout_s: float = 30.0,
        user_agent: str = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.user_agent = user_agent

    def pesquisar(self, termo: str, categorias: list[Any] | None = None) -> PortalTussSearchResult:
        categorias = categorias if categorias is not None else []
        body = json.dumps({"termo": termo, "categorias": categorias}, ensure_ascii=False).encode("utf-8")
        url = f"{self.base_url}{PESQUISAR_PATH}"
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
                "User-Agent": self.user_agent,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                status = resp.getcode()
                raw_txt = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            return PortalTussSearchResult(
                ok=False,
                http_status=e.code,
                total=0,
                first_codigo=None,
                first_descricao=None,
                first_vigencia=None,
                first_categoria=None,
                raw=None,
                error=f"HTTP {e.code}: {e.reason}",
            )
        except urllib.error.URLError as e:
            return PortalTussSearchResult(
                ok=False,
                http_status=None,
                total=0,
                first_codigo=None,
                first_descricao=None,
                first_vigencia=None,
                first_categoria=None,
                raw=None,
                error=str(e.reason),
            )

        try:
            raw: dict[str, Any] = json.loads(raw_txt)
        except json.JSONDecodeError:
            return PortalTussSearchResult(
                ok=False,
                http_status=status,
                total=0,
                first_codigo=None,
                first_descricao=None,
                first_vigencia=None,
                first_categoria=None,
                raw=None,
                error="Resposta não é JSON",
            )

        if raw.get("message") and raw.get("detail") is None and "pagina" not in raw:
            return PortalTussSearchResult(
                ok=False,
                http_status=status,
                total=0,
                first_codigo=None,
                first_descricao=None,
                first_vigencia=None,
                first_categoria=None,
                raw=raw,
                error=str(raw.get("message", ""))[:500],
            )

        pagina = raw.get("pagina") or {}
        total = int(pagina.get("total") or 0)
        rows = pagina.get("result") or []
        if not rows:
            return PortalTussSearchResult(
                ok=True,
                http_status=status,
                total=0,
                first_codigo=None,
                first_descricao=None,
                first_vigencia=None,
                first_categoria=None,
                raw=raw,
            )

        hit = rows[0]
        if not isinstance(hit, dict):
            hit = {}
        cat = hit.get("categoriaValue") or {}
        categoria = str(cat.get("text") or "") if isinstance(cat, dict) else ""
        extras = _flatten_first_hit(hit)
        return PortalTussSearchResult(
            ok=True,
            http_status=status,
            total=total,
            first_codigo=str(hit.get("codigoTUSS") or "") or None,
            first_descricao=str(hit.get("descricao") or "") or None,
            first_vigencia=str(hit.get("vigencia") or "") or None,
            first_categoria=categoria or None,
            raw=raw,
            extras=extras,
        )

    def pesquisar_com_retry(
        self,
        termo: str,
        *,
        categorias: list[Any] | None = None,
        max_tentativas: int = 4,
        backoff_inicial_s: float = 2.0,
    ) -> PortalTussSearchResult:
        ultimo: PortalTussSearchResult | None = None
        for tentativa in range(max_tentativas):
            r = self.pesquisar(termo, categorias)
            if r.ok and r.error is None:
                return r
            if r.http_status == 429 or (r.error and "429" in r.error):
                time.sleep(backoff_inicial_s * (2**tentativa) + random.uniform(0, 1))
            elif r.error and ("timed out" in r.error.lower() or "temporariamente" in r.error.lower()):
                time.sleep(backoff_inicial_s * (1.5**tentativa))
            elif tentativa < max_tentativas - 1:
                time.sleep(backoff_inicial_s * (1.2**tentativa))
            ultimo = r
        return ultimo or PortalTussSearchResult(
            ok=False, http_status=None, total=0,
            first_codigo=None, first_descricao=None, first_vigencia=None, first_categoria=None,
            raw=None, error="retry esgotado",
        )

    def pesquisar_codigo(
        self,
        codigo: str,
        *,
        delay_s: float = 0.0,
        max_tentativas: int = 4,
    ) -> PortalTussSearchResult:
        if delay_s > 0:
            time.sleep(delay_s)
        return self.pesquisar_com_retry(codigo.strip(), max_tentativas=max_tentativas)
