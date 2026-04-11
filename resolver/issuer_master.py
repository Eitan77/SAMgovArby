"""resolver/issuer_master.py — Build issuer_master and issuer_aliases from SEC + Nasdaq Trader."""
from __future__ import annotations
import hashlib, json, logging, re, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
import duckdb, requests
from resolver.normalize import conservative_normalize, aggressive_normalize
from resolver.models import DEFAULT_HTTP_HEADERS

log = logging.getLogger(__name__)

SEC_TICKERS_URL          = "https://www.sec.gov/files/company_tickers.json"
SEC_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL        = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL         = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

US_EXCHANGES = {"Nasdaq", "NYSE", "NYSEArca", "NYSEAmerican", "BATS", "CBOE"}

# These must match as whole words only to avoid "unit" matching "United", etc.
_DISALLOWED_WORDS = {
    "etf", "fund", "preferred", "warrant", "unit", "right", "note",
    "debenture", "bond", "reit", "spac", "trust", "depositary",
}
# These can remain as substring patterns (unambiguous)
_DISALLOWED_SUBSTRINGS = {
    "exchange-traded", "money market", "closed-end", "interval fund",
}


def is_eligible_common_equity(security_name: str | None, exchange: str | None) -> bool:
    if not security_name:
        return False
    lower = security_name.lower()
    for sub in _DISALLOWED_SUBSTRINGS:
        if sub in lower:
            return False
    # Word-boundary check using regex — avoids "unit" matching "United"
    words = set(re.findall(r"[a-z]+", lower))
    if words & _DISALLOWED_WORDS:
        return False
    if exchange and exchange not in US_EXCHANGES:
        return False
    return True


def _share_class_rank(ticker: str, security_name: str | None) -> int:
    sn = (security_name or "").upper()
    if "ADR" in sn or ticker.endswith("Y"):
        return 4
    if ticker.endswith("A") or "CLASS A" in sn:
        return 1
    if ticker.endswith("B") or "CLASS B" in sn:
        return 2
    return 3


def _alias_id(pub_id: str, raw: str, alias_type: str, source: str) -> str:
    return hashlib.md5(f"{pub_id}|{raw}|{alias_type}|{source}".encode()).hexdigest()[:16]


def _insert_alias(con, pub_id, raw, alias_type, source, valid_from=None, valid_to=None):
    if not raw or not str(raw).strip():
        return
    raw = str(raw).strip()
    cons = conservative_normalize(raw)
    agg  = aggressive_normalize(raw)
    aid  = _alias_id(pub_id, raw, alias_type, source)
    con.execute("""
        INSERT OR IGNORE INTO issuer_aliases
            (alias_id, public_company_id, alias_raw,
             alias_normalized_conservative, alias_normalized_aggressive,
             alias_type, source, valid_from, valid_to)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [aid, pub_id, raw, cons, agg, alias_type, source, valid_from, valid_to])


def build_issuer_master_from_sec(
    con: duckdb.DuckDBPyConnection,
    tickers_data: dict,
    exchange_data: dict,
) -> int:
    exch_lookup: dict[int, dict] = {}
    if "fields" in exchange_data and "data" in exchange_data:
        fields = exchange_data["fields"]
        for row in exchange_data["data"]:
            d = dict(zip(fields, row))
            cik_int = int(d.get("cik", 0) or 0)
            if cik_int:
                exch_lookup[cik_int] = d

    master_rows = []
    alias_rows: list[tuple] = []

    for _, entry in tickers_data.items():
        cik_int  = int(entry.get("cik_str", 0) or 0)
        ticker   = (entry.get("ticker") or "").strip().upper()
        name     = (entry.get("title") or "").strip()
        if not ticker or not name:
            continue
        cik_str  = str(cik_int).zfill(10)
        exch_row = exch_lookup.get(cik_int, {})
        exchange = exch_row.get("exchange", "")
        sec_name = exch_row.get("name") or name

        lower = sec_name.lower()
        words_set = set(re.findall(r"[a-z]+", lower))
        is_etf  = "etf" in words_set or "exchange-traded" in lower
        is_fund = ("fund" in words_set or "trust" in words_set) and not is_etf
        is_pref = "preferred" in words_set or "depositary" in words_set
        is_warr = "warrant" in words_set
        is_unit_flag = "unit" in words_set and "united" not in lower
        is_adr  = "adr" in words_set or "american depositary" in lower
        is_com  = is_eligible_common_equity(sec_name, exchange)

        pub_id = f"CIK_{cik_str}"
        master_rows.append((pub_id, "CIK", cik_str, sec_name, ticker, exchange,
                             exchange in US_EXCHANGES or not exchange, is_com,
                             _share_class_rank(ticker, sec_name),
                             is_adr, is_etf, is_fund, is_warr, is_unit_flag, is_pref,
                             "active", 1))
        for raw, atype, src in [(name, "current_name", "sec_tickers"),
                                (sec_name, "current_name", "sec_exchange"),
                                (ticker, "ticker_name", "sec_tickers")]:
            if raw and str(raw).strip():
                r = str(raw).strip()
                alias_rows.append((
                    _alias_id(pub_id, r, atype, src), pub_id, r,
                    conservative_normalize(r), aggressive_normalize(r),
                    atype, src, None, None
                ))

    # Bulk insert via pandas DataFrame registered as DuckDB relation (fastest path)
    import pandas as pd
    master_df = pd.DataFrame(master_rows, columns=[
        "public_company_id", "public_company_id_type", "cik",
        "issuer_name_current", "ticker_current", "exchange_current",
        "is_us_tradable", "is_common_equity", "share_class_rank",
        "is_adr", "is_etf", "is_fund", "is_warrant", "is_unit", "is_preferred",
        "active_status", "source_priority",
    ])
    con.register("_sec_master_tmp", master_df)
    con.execute("INSERT OR IGNORE INTO issuer_master SELECT * FROM _sec_master_tmp")
    con.unregister("_sec_master_tmp")

    alias_df = pd.DataFrame(alias_rows, columns=[
        "alias_id", "public_company_id", "alias_raw",
        "alias_normalized_conservative", "alias_normalized_aggressive",
        "alias_type", "source", "valid_from", "valid_to",
    ])
    con.register("_sec_alias_tmp", alias_df)
    con.execute("INSERT OR IGNORE INTO issuer_aliases SELECT * FROM _sec_alias_tmp")
    con.unregister("_sec_alias_tmp")

    inserted = len(master_rows)
    log.info(f"SEC: {inserted} issuers loaded, {len(alias_rows)} aliases")
    return inserted


def _parse_nasdaq_lines(text: str) -> list[dict]:
    rows = []
    lines = text.splitlines()
    if not lines:
        return rows
    headers = [h.strip() for h in lines[0].split("|")]
    for line in lines[1:]:
        if line.startswith("File Creation Time") or not line.strip():
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 2:
            rows.append(dict(zip(headers, parts)))
    return rows


def build_issuer_master_from_nasdaq(
    con: duckdb.DuckDBPyConnection,
    nasdaq_text: str,
    other_text: str,
) -> int:
    # Build ticker→pub_id lookup from existing master (already loaded from SEC)
    existing = {r[0]: r[1] for r in con.execute(
        "SELECT ticker_current, public_company_id FROM issuer_master"
    ).fetchall()}

    new_master: list[tuple] = []
    alias_rows: list[tuple] = []

    for row in _parse_nasdaq_lines(nasdaq_text) + _parse_nasdaq_lines(other_text):
        ticker   = (row.get("Symbol") or row.get("ACT Symbol") or "").strip().upper()
        sec_name = (row.get("Security Name") or "").strip()
        if not ticker or not sec_name:
            continue
        pub_id = existing.get(ticker)
        if pub_id is None:
            pub_id = f"NASDAQ_{ticker}"
            is_com = is_eligible_common_equity(sec_name, "Nasdaq")
            lower = sec_name.lower()
            new_master.append((pub_id, "INTERNAL", None, sec_name, ticker, "Nasdaq",
                                True, is_com, 3, False, "etf" in lower,
                                False, False, False, False, "active", 2))
            existing[ticker] = pub_id
        raw = sec_name
        alias_rows.append((
            _alias_id(pub_id, raw, "security_name", "nasdaq_trader"),
            pub_id, raw,
            conservative_normalize(raw), aggressive_normalize(raw),
            "security_name", "nasdaq_trader", None, None
        ))

    import pandas as pd
    if new_master:
        df = pd.DataFrame(new_master, columns=[
            "public_company_id", "public_company_id_type", "cik",
            "issuer_name_current", "ticker_current", "exchange_current",
            "is_us_tradable", "is_common_equity", "share_class_rank",
            "is_adr", "is_etf", "is_fund", "is_warrant", "is_unit", "is_preferred",
            "active_status", "source_priority",
        ])
        con.register("_nasdaq_master_tmp", df)
        con.execute("INSERT OR IGNORE INTO issuer_master SELECT * FROM _nasdaq_master_tmp")
        con.unregister("_nasdaq_master_tmp")
    if alias_rows:
        adf = pd.DataFrame(alias_rows, columns=[
            "alias_id", "public_company_id", "alias_raw",
            "alias_normalized_conservative", "alias_normalized_aggressive",
            "alias_type", "source", "valid_from", "valid_to",
        ])
        con.register("_nasdaq_alias_tmp", adf)
        con.execute("INSERT OR IGNORE INTO issuer_aliases SELECT * FROM _nasdaq_alias_tmp")
        con.unregister("_nasdaq_alias_tmp")

    log.info(f"Nasdaq Trader: {len(new_master)} new symbols, {len(alias_rows)} aliases")
    return len(new_master)


def _generate_acronym(name: str) -> str | None:
    """Generate first-letter acronym from words > 2 chars (skip stopwords)."""
    skip = {"and", "of", "the", "for", "a", "an", "in", "at", "by", "to", "or"}
    words = re.findall(r"[A-Za-z]+", name)
    meaningful = [w for w in words if len(w) > 2 and w.lower() not in skip]
    if len(meaningful) >= 2:
        return "".join(w[0].upper() for w in meaningful)
    return None


def _add_acronym_aliases(con: duckdb.DuckDBPyConnection) -> int:
    """Add unique first-letter acronym aliases. Batched for speed."""
    rows = con.execute(
        "SELECT public_company_id, issuer_name_current FROM issuer_master"
    ).fetchall()
    acronym_map: dict[str, list[str]] = {}
    for pub_id, name in rows:
        acr = _generate_acronym(name or "")
        if acr and len(acr) >= 2:
            acronym_map.setdefault(acr, []).append(pub_id)

    alias_rows = []
    for acr, pub_ids in acronym_map.items():
        if len(pub_ids) == 1 and len(acr) >= 4:  # Skip short 2-3 char acronyms to reduce false positives
            pub_id = pub_ids[0]
            alias_rows.append((
                _alias_id(pub_id, acr, "acronym", "derived"),
                pub_id, acr,
                conservative_normalize(acr), aggressive_normalize(acr),
                "acronym", "derived", None, None
            ))

    if alias_rows:
        import pandas as pd
        adf = pd.DataFrame(alias_rows, columns=[
            "alias_id", "public_company_id", "alias_raw",
            "alias_normalized_conservative", "alias_normalized_aggressive",
            "alias_type", "source", "valid_from", "valid_to",
        ])
        con.register("_acr_alias_tmp", adf)
        con.execute("INSERT OR IGNORE INTO issuer_aliases SELECT * FROM _acr_alias_tmp")
        con.unregister("_acr_alias_tmp")
    log.info(f"Acronym aliases: {len(alias_rows)} unique acronyms added")
    return len(alias_rows)


def _flush_alias_rows(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """Bulk-insert alias rows via pandas (fast path)."""
    import pandas as pd
    adf = pd.DataFrame(rows, columns=[
        "alias_id", "public_company_id", "alias_raw",
        "alias_normalized_conservative", "alias_normalized_aggressive",
        "alias_type", "source", "valid_from", "valid_to",
    ])
    con.register("_edgar_alias_tmp", adf)
    con.execute("INSERT OR IGNORE INTO issuer_aliases SELECT * FROM _edgar_alias_tmp")
    con.unregister("_edgar_alias_tmp")


_EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_EDGAR_RATE = 0.11  # seconds between dispatches (SEC limit: 10 req/s)
_edgar_last_req = 0.0
_EDGAR_RATE_LOCK = threading.Lock()


def _edgar_get(url: str, headers: dict) -> dict | None:
    global _edgar_last_req
    # Serialize dispatch timing; I/O happens concurrently after lock release.
    with _EDGAR_RATE_LOCK:
        elapsed = time.time() - _edgar_last_req
        if elapsed < _EDGAR_RATE:
            time.sleep(_EDGAR_RATE - elapsed)
        _edgar_last_req = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.debug(f"EDGAR fetch failed {url}: {e}")
    return None


def _load_cik_former_names(args: tuple) -> list[tuple]:
    """Worker: read one CIK's cached submission file and return alias tuples.
    Pure function — no DB access, safe for thread pool."""
    pub_id, cik, subs_dir, headers = args
    cik_padded = str(cik).zfill(10)
    cache_file = Path(subs_dir) / f"{cik_padded}.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text())
        except Exception:
            return []
    else:
        url = _EDGAR_SUBMISSIONS_URL.format(cik=cik_padded)
        data = _edgar_get(url, headers)
        if data:
            # Write atomically: temp file + rename to avoid corrupt partial files
            tmp = cache_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, separators=(",", ":")))
            tmp.replace(cache_file)
        else:
            return []
    result = []
    for fn in data.get("formerNames", []):
        old_name = (fn.get("name") or "").strip()
        date_val = fn.get("date")
        if old_name:
            result.append((
                _alias_id(pub_id, old_name, "former_name", "edgar_submissions"),
                pub_id, old_name,
                conservative_normalize(old_name), aggressive_normalize(old_name),
                "former_name", "edgar_submissions", None, date_val,
            ))
    return result


def enrich_with_edgar_former_names(
    con: duckdb.DuckDBPyConnection,
    cache_dir: str = "data/cache",
    max_ciks: int = 5000,
    workers: int = 16,
) -> int:
    """Fetch EDGAR submissions for all CIK-based issuers and add formerNames as aliases.
    Results are disk-cached. File reads are parallelised with a thread pool.
    Pass max_ciks=0 for all."""
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    subs_dir = Path(cache_dir) / "edgar_subs"
    subs_dir.mkdir(exist_ok=True)

    rows = con.execute(
        "SELECT public_company_id, cik FROM issuer_master "
        "WHERE public_company_id_type='CIK' AND cik IS NOT NULL"
    ).fetchall()
    if max_ciks:
        rows = rows[:max_ciks]

    headers = {**DEFAULT_HTTP_HEADERS, "Accept": "application/json"}
    work = [(pub_id, cik, str(subs_dir), headers) for pub_id, cik in rows]

    alias_rows: list[tuple] = []
    added_total = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for batch_result in pool.map(_load_cik_former_names, work):
            alias_rows.extend(batch_result)
            added_total += len(batch_result)
            if len(alias_rows) >= 4000:
                _flush_alias_rows(con, alias_rows)
                alias_rows.clear()

    if alias_rows:
        _flush_alias_rows(con, alias_rows)

    log.info(f"EDGAR former names: {added_total} aliases added from {len(rows)} CIKs")
    return added_total


def build_issuer_master_from_fixtures(
    con: duckdb.DuckDBPyConnection,
    tickers_data: dict,
    exchange_data: dict,
    nasdaq_text: str,
    other_text: str,
) -> None:
    build_issuer_master_from_sec(con, tickers_data, exchange_data)
    build_issuer_master_from_nasdaq(con, nasdaq_text, other_text)
    _add_acronym_aliases(con)


def _fetch_json(url: str, cache_dir: str | None) -> Any:
    if cache_dir:
        fname = hashlib.md5(url.encode()).hexdigest() + ".json"
        p = Path(cache_dir) / fname
        if p.exists():
            return json.loads(p.read_text())
    log.info(f"GET {url}")
    r = requests.get(url, headers=DEFAULT_HTTP_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if cache_dir:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        (Path(cache_dir) / fname).write_text(json.dumps(data))
    return data


_TEXT_HEADERS = {
    "User-Agent": DEFAULT_HTTP_HEADERS["User-Agent"],
    "Accept": "text/plain,text/html,*/*",
}


def _fetch_text(url: str, cache_dir: str | None) -> str:
    if cache_dir:
        fname = hashlib.md5(url.encode()).hexdigest() + ".txt"
        p = Path(cache_dir) / fname
        if p.exists():
            return p.read_text()
    log.info(f"GET {url}")
    r = requests.get(url, headers=_TEXT_HEADERS, timeout=30)
    r.raise_for_status()
    text = r.text
    if cache_dir:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        (Path(cache_dir) / fname).write_text(text)
    return text


def refresh_issuer_master(
    con: duckdb.DuckDBPyConnection,
    cache_dir: str = "data/cache",
    force: bool = False,
    enrich_edgar: bool = True,
) -> str:
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    cd = None if force else cache_dir
    tickers  = _fetch_json(SEC_TICKERS_URL, cd)
    exchange = _fetch_json(SEC_TICKERS_EXCHANGE_URL, cd)
    nasdaq   = _fetch_text(NASDAQ_LISTED_URL, cd)
    other    = _fetch_text(OTHER_LISTED_URL, cd)
    con.execute("DELETE FROM issuer_aliases")
    con.execute("DELETE FROM issuer_master")
    build_issuer_master_from_fixtures(con, tickers, exchange, nasdaq, other)
    if enrich_edgar:
        enrich_with_edgar_former_names(con, cache_dir, max_ciks=0)
    version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info(f"Issuer master refreshed. Version: {version}")
    return version


def get_issuer_master_version(con: duckdb.DuckDBPyConnection) -> str:
    try:
        n = con.execute("SELECT COUNT(*) FROM issuer_master").fetchone()[0]
        return f"rows={n}"
    except Exception:
        return "unknown"
