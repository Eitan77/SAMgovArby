"""resolver/pipeline.py — 8-stage V1 resolution pipeline."""
from __future__ import annotations
import json, logging, re, threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple
import duckdb
from resolver.normalize import conservative_normalize, aggressive_normalize, token_metadata
from resolver.models import score_to_confidence_band, V1ThresholdsConfig, RESOLVER_V1_VERSION

log = logging.getLogger(__name__)

# ── In-memory alias index (built once, used by stage 5) ───────────────────────

class _AliasRow(NamedTuple):
    pub_id:     str
    name:       str
    ticker:     str
    exchange:   str
    is_com:     bool
    alias_cons: str
    rank:       int

# token → list[_AliasRow]  (only common-equity, active issuers)
_TOKEN_INDEX: dict[str, list[_AliasRow]] = {}
# exact conservative alias → deduped list of (pub_id, name, ticker, exchange, alias_cons)
_EXACT_CONS: dict[str, list[tuple]] = {}
# exact aggressive alias → deduped list
_EXACT_AGG: dict[str, list[tuple]] = {}
_ALIAS_INDEX_DB: str = ""   # path of DB the index was built from


def _db_path(con: duckdb.DuckDBPyConnection) -> str:
    """Return the stable file-path key for a connection.
    Uses the storage connection pool to map con → path without relying on repr()."""
    try:
        from resolver.storage import _connections
        for path, c in _connections.items():
            if c is con:
                return path
    except ImportError:
        pass
    # Fallback: str(con) — works for in-memory DBs in tests
    return str(con)


def _build_alias_index(con: duckdb.DuckDBPyConnection) -> None:
    """Load all common-equity aliases into in-memory indexes (built once per DB).
    Used by stages 3, 4, 5.  ~51 K aliases, ~15 MB total."""
    global _TOKEN_INDEX, _EXACT_CONS, _EXACT_AGG, _ALIAS_INDEX_DB
    db_path = _db_path(con)
    # Guard: skip rebuild if the same DB is already loaded.
    # Check _ALIAS_INDEX_DB != "" (not just _TOKEN_INDEX) so all three dicts are validated.
    if _ALIAS_INDEX_DB != "" and _ALIAS_INDEX_DB == db_path:
        return
    rows = con.execute("""
        SELECT ia.public_company_id, im.issuer_name_current,
               im.ticker_current, im.exchange_current, im.is_common_equity,
               ia.alias_normalized_conservative, ia.alias_normalized_aggressive,
               im.share_class_rank
        FROM issuer_aliases ia
        JOIN issuer_master im ON ia.public_company_id = im.public_company_id
        WHERE im.is_common_equity = TRUE AND im.active_status = 'active'
    """).fetchall()

    tok_idx: dict[str, list[_AliasRow]] = {}
    cons_idx: dict[str, dict[str, tuple]] = {}  # cons → {pub_id → row}
    agg_idx:  dict[str, dict[str, tuple]] = {}  # agg  → {pub_id → row}

    for pub_id, name, ticker, exchange, is_com, alias_cons, alias_agg, rank in rows:
        name = name or ""; ticker = ticker or ""; exchange = exchange or ""
        is_com = bool(is_com); rank = rank or 3

        if alias_cons:
            ar = _AliasRow(pub_id, name, ticker, exchange, is_com, alias_cons, rank)
            for tok in alias_cons.split():
                tok_idx.setdefault(tok, []).append(ar)
            row_tuple = (pub_id, name, ticker, exchange, alias_cons)
            if alias_cons not in cons_idx:
                cons_idx[alias_cons] = {}
            if pub_id not in cons_idx[alias_cons]:  # keep first (best rank)
                cons_idx[alias_cons][pub_id] = row_tuple

        if alias_agg:
            row_tuple_agg = (pub_id, name, ticker, exchange, alias_agg)
            if alias_agg not in agg_idx:
                agg_idx[alias_agg] = {}
            if pub_id not in agg_idx[alias_agg]:
                agg_idx[alias_agg][pub_id] = row_tuple_agg

    _TOKEN_INDEX = tok_idx
    _EXACT_CONS  = {k: list(v.values()) for k, v in cons_idx.items()}
    _EXACT_AGG   = {k: list(v.values()) for k, v in agg_idx.items()}
    _ALIAS_INDEX_DB = db_path
    log.debug(f"Alias index built: {len(tok_idx)} tokens, {len(cons_idx)} cons, {len(agg_idx)} agg")

# Generic corporate words that are NOT distinctive for matching purposes.
# A query with only these words cannot reliably identify a unique company.
_GENERIC_TOKENS = {
    # Corporate structure words
    "ENTERPRISES", "SOLUTIONS", "TECHNOLOGIES", "SERVICES", "SYSTEMS",
    "INDUSTRIES", "MANAGEMENT", "ASSOCIATES", "PARTNERS", "CONSULTING",
    "RESOURCES", "OPERATIONS", "DEVELOPMENT", "CONSTRUCTION", "ENGINEERING",
    "LOGISTICS", "COMMUNICATIONS", "NETWORKS", "VENTURES", "STAFFING",
    "INNOVATIONS", "PROFESSIONALS", "CONTRACTORS", "SUPPLIES",
    # Generic geographic/descriptive adjectives
    "INTERNATIONAL", "GLOBAL", "NATIONAL", "FEDERAL", "AMERICAN", "UNITED",
    "NORTH", "SOUTH", "EAST", "WEST", "CENTRAL",
    # Generic functional words
    "GENERAL", "ADVANCED", "INTEGRATED", "STRATEGIC", "PREMIER", "PREFERRED",
    "TECHNICAL", "SUPPORT", "ENERGY", "DEFENSE", "SECURITY", "HEALTH",
    "MEDICAL", "FINANCIAL", "DIGITAL", "TECHNOLOGY", "HEALTHCARE",
    # Generic standalone corporate words (too common for token search)
    "COMPANY", "BUSINESS", "CAPITAL", "HOLDINGS",
}


@dataclass
class ClusterContext:
    cluster_id:             str
    canonical_parent_name:  str | None
    canonical_entity_name:  str | None
    uei:                    str | None
    parent_uei:             str | None
    cage:                   str | None
    parent_name_norm:       str | None  # conservative
    legal_name_norm:        str | None  # conservative
    all_parent_names:       list[str] = field(default_factory=list)
    all_legal_names:        list[str] = field(default_factory=list)
    all_dba_names:          list[str] = field(default_factory=list)


def _result(cluster_id, stage, pub_id, pub_name, ticker, exchange, relationship,
            score, explanation, matched_entity=None, matched_parent=None,
            matched_alias=None, override=False, ambiguous=False,
            needs_review=False, share_class_rule=None, evidence=None) -> dict:
    return {
        "entity_cluster_id":      cluster_id,
        "resolved":               ticker is not None,
        "public_company_id":      pub_id,
        "public_company_id_type": "CIK" if (pub_id or "").startswith("CIK_") else "INTERNAL",
        "public_company_name":    pub_name,
        "preferred_ticker":       ticker,
        "preferred_exchange":     exchange,
        "relationship_type":      relationship,
        "resolution_stage":       stage,
        "confidence_score":       score,
        "confidence_band":        score_to_confidence_band(score),
        "match_explanation":      explanation,
        "matched_entity_name":    matched_entity,
        "matched_parent_name":    matched_parent,
        "matched_alias":          matched_alias,
        "manual_override_used":   override,
        "ambiguous":              ambiguous,
        "needs_review":           needs_review,
        "share_class_rule_used":  share_class_rule or "issuer_master_rank",
        "historical_ticker_attempted": False,
        "ticker_as_of_award_date":     None,
        "ticker_as_of_award_confidence": None,
        "source_evidence_json":   json.dumps(evidence or {}),
    }


def _unresolved(cluster_id: str, reason: str = "no_match") -> dict:
    return _result(cluster_id, "unresolved", None, None, None, None,
                   "unresolved", 0.0, reason)


def _ambiguous(cluster_id: str, top_candidates: list[dict]) -> dict:
    return _result(cluster_id, "ambiguous", None, None, None, None,
                   "ambiguous", 0.0, "multiple_near_equal_candidates",
                   ambiguous=True, needs_review=True,
                   evidence={"top": [{"ticker": c.get("ticker_current"),
                                      "score": c.get("score")} for c in top_candidates[:3]]})


# ── Stage 0: Manual overrides (in-memory cache) ───────────────────────────────

_OVERRIDE_CACHE: dict[tuple, tuple] | None = None  # (ktype, kval) → row tuple
_OVERRIDE_CACHE_DB: str = ""


def _load_override_cache(con: duckdb.DuckDBPyConnection) -> None:
    global _OVERRIDE_CACHE, _OVERRIDE_CACHE_DB
    db_path = _db_path(con)
    if _OVERRIDE_CACHE_DB == db_path and _OVERRIDE_CACHE is not None:
        return
    rows = con.execute("""
        SELECT override_key_type, override_key_value,
               public_company_id, public_company_name,
               preferred_ticker, preferred_exchange, relationship_type
        FROM manual_overrides WHERE active=TRUE
    """).fetchall()
    _OVERRIDE_CACHE = {(r[0], r[1]): r[2:] for r in rows}
    _OVERRIDE_CACHE_DB = db_path


def stage0_override(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    _load_override_cache(con)
    assert _OVERRIDE_CACHE is not None
    checks = []
    if ctx.parent_uei:       checks.append(("ultimate_parent_uei", ctx.parent_uei))
    if ctx.uei:              checks.append(("uei",                 ctx.uei))
    if ctx.cage:             checks.append(("cage",                ctx.cage))
    if ctx.parent_name_norm: checks.append(("parent_name_norm",    ctx.parent_name_norm))
    if ctx.legal_name_norm:  checks.append(("legal_name_norm",     ctx.legal_name_norm))
    for ktype, kval in checks:
        row = _OVERRIDE_CACHE.get((ktype, kval))
        if row:
            pub_id, pub_name, ticker, exchange, rel = row
            return _result(ctx.cluster_id, "stage0_override",
                           pub_id, pub_name, ticker, exchange,
                           rel or "direct_public_awardee", 100.0,
                           f"override:{ktype}={kval}", override=True,
                           evidence={"override_key_type": ktype, "override_key_value": kval})
    return None


# ── Stage 1: Resolution cache (in-memory) ────────────────────────────────────

_RES_CACHE: dict[str, tuple] | None = None   # cluster_id → row tuple
_RES_CACHE_DB: str = ""
_RES_CACHE_PENDING: list[tuple] = []          # rows waiting to be flushed
_RES_CACHE_LOCK = threading.Lock()            # guards _RES_CACHE_PENDING writes/flushes


def _load_resolution_cache(con: duckdb.DuckDBPyConnection) -> None:
    """Load entire resolution_cache table into memory (one round-trip)."""
    global _RES_CACHE, _RES_CACHE_DB
    db_path = _db_path(con)
    if _RES_CACHE_DB == db_path and _RES_CACHE is not None:
        return
    rows = con.execute("""
        SELECT entity_cluster_id, resolved, public_company_id, public_company_name,
               preferred_ticker, preferred_exchange, relationship_type,
               resolution_stage, confidence_score, confidence_band,
               match_explanation, source_evidence_json
        FROM resolution_cache
    """).fetchall()
    _RES_CACHE = {r[0]: r[1:] for r in rows}
    _RES_CACHE_DB = db_path


def invalidate_resolution_cache() -> None:
    """Force reload on next access (call after bulk writes or DELETE)."""
    global _RES_CACHE, _RES_CACHE_DB
    _RES_CACHE = None
    _RES_CACHE_DB = ""


def invalidate_alias_index() -> None:
    """Force rebuild of the in-memory alias index (call after issuer_master refresh)."""
    global _ALIAS_INDEX_DB, _TOKEN_INDEX, _EXACT_CONS, _EXACT_AGG
    _ALIAS_INDEX_DB = ""
    _TOKEN_INDEX = {}
    _EXACT_CONS = {}
    _EXACT_AGG = {}


def stage1_cache(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    _load_resolution_cache(con)
    assert _RES_CACHE is not None
    row = _RES_CACHE.get(ctx.cluster_id)
    if not row:
        return None
    (resolved, pub_id, pub_name, ticker, exchange, rel,
     stage, score, band, explanation, evidence) = row
    return {
        "entity_cluster_id":      ctx.cluster_id,
        "resolved":               bool(resolved),
        "public_company_id":      pub_id,
        "public_company_id_type": "CIK" if (pub_id or "").startswith("CIK_") else "INTERNAL",
        "public_company_name":    pub_name,
        "preferred_ticker":       ticker,
        "preferred_exchange":     exchange,
        "relationship_type":      rel,
        "resolution_stage":       f"cache({stage})",
        "confidence_score":       float(score or 0),
        "confidence_band":        band or score_to_confidence_band(float(score or 0)),
        "match_explanation":      explanation,
        "manual_override_used":   False,
        "ambiguous":              False,
        "needs_review":           False,
        "share_class_rule_used":  "cached",
        "historical_ticker_attempted": False,
        "ticker_as_of_award_date":     None,
        "ticker_as_of_award_confidence": None,
        "source_evidence_json":   evidence or "{}",
    }


# ── Shared: exact alias lookup (in-memory, O(1)) ─────────────────────────────

def _lookup_exact(name_cons: str | None, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Return distinct (pub_id, name, ticker, exchange, alias) rows for an exact conservative match."""
    if not name_cons:
        return []
    _build_alias_index(con)
    return _EXACT_CONS.get(name_cons, [])


def _lookup_exact_agg(name_agg: str | None, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Return distinct (pub_id, name, ticker, exchange, alias) rows for an exact aggressive match."""
    if not name_agg:
        return []
    _build_alias_index(con)
    return _EXACT_AGG.get(name_agg, [])


# ── Stage 2: Exact parent match ───────────────────────────────────────────────

def stage2_exact_parent(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    search = ctx.parent_name_norm or conservative_normalize(ctx.canonical_parent_name)
    rows = _lookup_exact(search, con)
    if len(rows) == 1:
        pub_id, pub_name, ticker, exchange, alias = rows[0]
        return _result(ctx.cluster_id, "stage2_exact_parent",
                       pub_id, pub_name, ticker, exchange,
                       "public_ultimate_parent", 97.0,
                       f"exact_parent:{alias}",
                       matched_parent=ctx.canonical_parent_name,
                       matched_alias=alias,
                       evidence={"alias": alias, "pub_id": pub_id})
    return None


# ── Stage 3: Exact direct-entity match ───────────────────────────────────────

def stage3_exact_direct(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    names = []
    if ctx.legal_name_norm:
        names.append(ctx.legal_name_norm)
    for n in ctx.all_legal_names + ctx.all_dba_names:
        c = conservative_normalize(n)
        if c and c not in names:
            names.append(c)
    for search in names:
        rows = _lookup_exact(search, con)
        if len(rows) == 1:
            pub_id, pub_name, ticker, exchange, alias = rows[0]
            score = (88.0 if ctx.parent_name_norm and
                     conservative_normalize(pub_name) != ctx.parent_name_norm else 92.0)
            return _result(ctx.cluster_id, "stage3_exact_direct",
                           pub_id, pub_name, ticker, exchange,
                           "direct_public_awardee", score,
                           f"exact_direct:{alias}",
                           matched_entity=ctx.canonical_entity_name,
                           matched_alias=alias,
                           evidence={"searched": search, "alias": alias})
    return None


# ── Stage 4: Deterministic alias match (aggressive) ──────────────────────────

def stage4_alias_match(ctx: ClusterContext, con: duckdb.DuckDBPyConnection) -> dict | None:
    all_names = (
        ([ctx.canonical_parent_name] if ctx.canonical_parent_name else []) +
        ([ctx.canonical_entity_name] if ctx.canonical_entity_name else []) +
        ctx.all_legal_names + ctx.all_parent_names + ctx.all_dba_names
    )
    for raw in all_names:
        agg = aggressive_normalize(raw)
        if not agg:
            continue
        # Short aggressive-normalized forms (≤3 chars) are ambiguous — skip unless
        # the original raw name is itself very short OR the form contains a digit
        # (digit-bearing names like "3M" are genuine brands, not generic acronyms)
        if len(agg) <= 3 and len(raw.split()) > 1 and not re.search(r"\d", agg):
            continue
        rows = _lookup_exact_agg(agg, con)
        if len(rows) == 1:
            pub_id, pub_name, ticker, exchange, alias = rows[0]
            rel = ("public_ultimate_parent" if raw == ctx.canonical_parent_name
                   else "direct_public_awardee")
            return _result(ctx.cluster_id, "stage4_alias",
                           pub_id, pub_name, ticker, exchange, rel, 85.0,
                           f"agg_alias:{alias}",
                           matched_alias=alias,
                           evidence={"agg": agg, "raw": raw})
    return None


# ── Stage 5: Candidate generation (in-memory token index) ────────────────────

def stage5_generate_candidates(
    ctx: ClusterContext,
    con: duckdb.DuckDBPyConnection,
    max_candidates: int = 20,
) -> list[dict]:
    """Generate candidate issuers via in-memory inverted token index (no SQL LIKE)."""
    _build_alias_index(con)  # no-op if already built

    # Use canonical names first; supplement with all cluster names for richer token coverage
    primary_names = [n for n in [ctx.canonical_parent_name, ctx.canonical_entity_name] if n]
    extra_names   = [n for n in ctx.all_parent_names + ctx.all_legal_names + ctx.all_dba_names
                     if n and n not in primary_names]
    query_names   = primary_names + extra_names[:4]  # cap extras to limit noise
    all_tokens: set[str] = set()
    for name in query_names:
        for t in token_metadata(name)["tokens"]:
            if len(t) > 3 and t not in _GENERIC_TOKENS:
                all_tokens.add(t)
    if not all_tokens:
        # Fallback: any token len > 3
        for name in query_names:
            for t in token_metadata(name)["tokens"]:
                if len(t) > 3:
                    all_tokens.add(t)
    if not all_tokens:
        return []

    # Use top 3 most-distinctive search tokens for index lookup
    search_tokens = sorted(all_tokens, key=len, reverse=True)[:3]

    # Collect candidate alias rows from the in-memory index
    best_per_pub: dict[str, tuple] = {}  # pub_id → (ratio, candidate_dict)
    for tok in search_tokens:
        for ar in _TOKEN_INDEX.get(tok, []):
            if ar.pub_id in best_per_pub and best_per_pub[ar.pub_id][0] >= 1.0:
                continue  # already perfect ratio
            alias_tokens = set(ar.alias_cons.split())
            overlap = len(all_tokens & alias_tokens)
            if overlap == 0:
                continue
            ratio = overlap / max(len(all_tokens), len(alias_tokens), 1)
            if ratio >= 0.25:
                prev = best_per_pub.get(ar.pub_id)
                if prev is None or ratio > prev[0]:
                    best_per_pub[ar.pub_id] = (ratio, {
                        "public_company_id":             ar.pub_id,
                        "issuer_name_current":           ar.name,
                        "ticker_current":                ar.ticker,
                        "exchange_current":              ar.exchange,
                        "is_common_equity":              ar.is_com,
                        "alias_normalized_conservative": ar.alias_cons,
                        "_overlap":                      ratio,
                    })

    scored = sorted(best_per_pub.values(), key=lambda x: x[0], reverse=True)
    return [v[1] for v in scored[:max_candidates]]


# ── Stage 6: Fuzzy scoring ────────────────────────────────────────────────────

def stage6_fuzzy_score(ctx: ClusterContext, candidates: list[dict]) -> list[dict]:
    """Score candidates 0-100 where a near-perfect name match reaches 85+."""
    try:
        from rapidfuzz import fuzz
    except ImportError:
        log.warning("rapidfuzz not installed; stage6 skipped")
        return []

    q_parent = conservative_normalize(ctx.canonical_parent_name) or ""
    q_entity = conservative_normalize(ctx.canonical_entity_name) or ""
    q_primary = q_parent or q_entity  # best query string

    # Compute distinctive tokens (long, not generic corporate words)
    all_q_names = [n for n in [ctx.canonical_parent_name, ctx.canonical_entity_name] if n]
    q_tokens: set[str] = set()
    for n in all_q_names:
        q_tokens.update(token_metadata(n)["tokens"])
    # Rare = distinctive (len > 5, not a generic corporate word)
    rare_q = {t for t in q_tokens if len(t) > 5 and t not in _GENERIC_TOKENS}

    scored = []
    for c in candidates:
        alias = c.get("alias_normalized_conservative") or ""
        issuer_name = c.get("issuer_name_current") or alias
        score = 0.0
        parts = []

        # Primary fuzzy similarity — 0 to 55 points
        # token_set_ratio handles extra suffix words (e.g. "ACUITY INC" ⊂ "ACUITY BRANDS INC")
        best_sim = 0.0
        for q in [q_parent, q_entity]:
            if q:
                best_sim = max(best_sim, fuzz.token_set_ratio(q, alias) / 100.0)
        score += best_sim * 55
        parts.append(f"sim={best_sim:.2f}")

        # Partial ratio bonus — catches substring containment (0 to 15 points)
        best_pr = 0.0
        for q in [q_parent, q_entity]:
            if q:
                best_pr = max(best_pr, fuzz.partial_ratio(q, alias) / 100.0)
        if best_pr:
            score += best_pr * 15
            parts.append(f"partial={best_pr:.2f}")

        # Distinctive token overlap (0 to 15 points)
        a_tokens = set(token_metadata(issuer_name)["tokens"])
        if rare_q and a_tokens:
            rare_match = len(rare_q & a_tokens) / max(len(rare_q), 1)
            score += rare_match * 15
            parts.append(f"rare={rare_match:.2f}")
        elif not rare_q:
            # Query has NO distinctive tokens (e.g. "DD ENTERPRISES INC") —
            # generic-word-only match can't reliably identify a company.
            score -= 30
            parts.append("no_distinctive_tokens")

        # Common equity bonus (0 to 5 points)
        if c.get("is_common_equity"):
            score += 5.0

        # Penalty: if candidate has many more tokens than query (dilution)
        if q_tokens and a_tokens:
            excess = max(0, len(a_tokens) - len(q_tokens) - 2)
            score -= min(excess * 2, 10)

        score = max(0.0, min(100.0, score))
        scored.append({**c, "score": round(score, 2), "explanation": " ".join(parts)})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


# ── Stage 7: Accept / ambiguous / unresolved ─────────────────────────────────

def stage7_accept(
    cluster_id: str,
    scored: list[dict],
    thresholds: V1ThresholdsConfig,
) -> dict:
    if not scored:
        return _unresolved(cluster_id, "no_candidates")
    top    = scored[0]
    runner = scored[1] if len(scored) > 1 else None
    margin = top["score"] - (runner["score"] if runner else 0.0)

    if top["score"] >= thresholds.auto_accept_min_score and margin >= thresholds.auto_accept_margin:
        return _result(cluster_id, "stage7_fuzzy",
                       top["public_company_id"], top["issuer_name_current"],
                       top["ticker_current"], top["exchange_current"],
                       "direct_public_awardee", top["score"],
                       top.get("explanation", ""),
                       matched_entity=top["issuer_name_current"],
                       evidence={"top_score": top["score"], "margin": margin})

    if (top["score"] >= thresholds.fuzzy_min_score and runner
            and margin < thresholds.auto_accept_margin):
        return _ambiguous(cluster_id, scored)

    return _unresolved(cluster_id, f"score={top['score']:.1f}")


# ── Cache write ───────────────────────────────────────────────────────────────

_RCACHE_COLS = [
    "entity_cluster_id", "resolved", "public_company_id", "public_company_name",
    "preferred_ticker", "preferred_exchange", "relationship_type", "resolution_stage",
    "confidence_score", "confidence_band", "match_explanation", "source_evidence_json",
    "resolver_version", "issuer_master_version", "first_resolved_at", "last_validated_at",
]


def write_resolution_cache(
    cluster_id: str,
    result: dict,
    con: duckdb.DuckDBPyConnection,
    issuer_master_version: str = "unknown",
) -> None:
    """Queue a cache write. Call flush_resolution_cache_pending() to commit in bulk."""
    now = datetime.utcnow().isoformat()
    row = (
        cluster_id,
        result.get("resolved", False),
        result.get("public_company_id"),
        result.get("public_company_name"),
        result.get("preferred_ticker"),
        result.get("preferred_exchange"),
        result.get("relationship_type"),
        result.get("resolution_stage"),
        result.get("confidence_score", 0.0),
        result.get("confidence_band", "low"),
        result.get("match_explanation"),
        result.get("source_evidence_json", "{}"),
        RESOLVER_V1_VERSION,
        issuer_master_version,
        now,
        now,
    )
    with _RES_CACHE_LOCK:
        _RES_CACHE_PENDING.append(row)
    # Also update in-memory cache so subsequent stage1_cache hits see fresh data
    if _RES_CACHE is not None:
        _RES_CACHE[cluster_id] = (
            result.get("resolved", False),
            result.get("public_company_id"),
            result.get("public_company_name"),
            result.get("preferred_ticker"),
            result.get("preferred_exchange"),
            result.get("relationship_type"),
            result.get("resolution_stage"),
            result.get("confidence_score", 0.0),
            result.get("confidence_band", "low"),
            result.get("match_explanation"),
            result.get("source_evidence_json", "{}"),
        )


def flush_resolution_cache_pending(con: duckdb.DuckDBPyConnection) -> int:
    """Flush all pending cache writes via pandas bulk insert (single round-trip)."""
    global _RES_CACHE_PENDING
    with _RES_CACHE_LOCK:
        if not _RES_CACHE_PENDING:
            return 0
        rows = _RES_CACHE_PENDING
        _RES_CACHE_PENDING = []
    import pandas as pd
    df = pd.DataFrame(rows, columns=_RCACHE_COLS)
    con.register("_rcache_flush_tmp", df)
    con.execute("INSERT OR REPLACE INTO resolution_cache SELECT * FROM _rcache_flush_tmp")
    con.unregister("_rcache_flush_tmp")
    return len(rows)


# ── Full cluster orchestrator ─────────────────────────────────────────────────

def resolve_cluster(
    ctx: ClusterContext,
    con: duckdb.DuckDBPyConnection,
    thresholds: V1ThresholdsConfig | None = None,
    issuer_master_version: str = "unknown",
) -> dict:
    """Run all stages for one cluster. Write cache. Return result dict."""
    if thresholds is None:
        thresholds = V1ThresholdsConfig()

    for stage_fn in (stage0_override, stage1_cache, stage2_exact_parent,
                     stage3_exact_direct, stage4_alias_match):
        result = stage_fn(ctx, con)
        if result:
            if stage_fn is not stage1_cache:
                write_resolution_cache(ctx.cluster_id, result, con, issuer_master_version)
            return result

    # Stages 5–7 fuzzy
    name_for_tokens = ctx.canonical_parent_name or ctx.canonical_entity_name or ""
    if len(name_for_tokens.split()) >= thresholds.min_tokens_for_fuzzy:
        candidates = stage5_generate_candidates(ctx, con, thresholds.max_candidates)
        scored     = stage6_fuzzy_score(ctx, candidates)
        result     = stage7_accept(ctx.cluster_id, scored, thresholds)
    else:
        result = _unresolved(ctx.cluster_id, "insufficient_tokens")

    write_resolution_cache(ctx.cluster_id, result, con, issuer_master_version)
    return result
