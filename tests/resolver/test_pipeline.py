"""tests/resolver/test_pipeline.py"""
import os, tempfile, uuid
import duckdb
from resolver.storage import get_db, ensure_schema
from resolver.normalize import conservative_normalize, aggressive_normalize
from resolver.pipeline import (
    ClusterContext, stage0_override, stage1_cache,
    stage2_exact_parent, stage3_exact_direct, stage4_alias_match,
    stage5_generate_candidates, stage6_fuzzy_score,
    stage7_accept, resolve_cluster,
)
from resolver.models import V1ThresholdsConfig


def _con():
    d = tempfile.mkdtemp()
    con = get_db(os.path.join(d, "t.duckdb"))
    ensure_schema(con)
    return con


def _seed_issuer(con, pub_id="CIK_0012345", name="Acme Corp", ticker="ACME", exchange="Nasdaq"):
    from resolver.issuer_master import _alias_id
    con.execute("""
        INSERT OR IGNORE INTO issuer_master (
            public_company_id, public_company_id_type, cik,
            issuer_name_current, ticker_current, exchange_current,
            is_us_tradable, is_common_equity, share_class_rank,
            is_adr, is_etf, is_fund, is_warrant, is_unit, is_preferred,
            active_status, source_priority
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [pub_id, "CIK", None, name, ticker, exchange,
          True, True, 1, False, False, False, False, False, False, "active", 1])
    cons = conservative_normalize(name)
    agg  = aggressive_normalize(name)
    aid  = _alias_id(pub_id, name, "current_name", "test")
    con.execute("""
        INSERT OR IGNORE INTO issuer_aliases
            (alias_id, public_company_id, alias_raw,
             alias_normalized_conservative, alias_normalized_aggressive,
             alias_type, source, valid_from, valid_to)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [aid, pub_id, name, cons, agg, "current_name", "test", None, None])


def _ctx(cluster_id="c1", parent_name=None, entity_name="Acme Corp",
         uei=None, parent_uei=None, cage=None):
    return ClusterContext(
        cluster_id=cluster_id,
        canonical_parent_name=parent_name,
        canonical_entity_name=entity_name,
        uei=uei, parent_uei=parent_uei, cage=cage,
        parent_name_norm=conservative_normalize(parent_name),
        legal_name_norm=conservative_normalize(entity_name),
    )


# ── Stage 0 ───────────────────────────────────────────────────────────────────

def test_stage0_uei_override():
    con = _con()
    con.execute("""
        INSERT INTO manual_overrides
            (override_id, override_key_type, override_key_value,
             public_company_id, public_company_name, preferred_ticker,
             preferred_exchange, relationship_type, active)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, [str(uuid.uuid4()), "uei", "UEI_ABC",
          "CIK_X", "Acme", "ACME", "Nasdaq", "direct_public_awardee", True])
    result = stage0_override(_ctx(uei="UEI_ABC"), con)
    assert result is not None
    assert result["preferred_ticker"] == "ACME"
    assert result["manual_override_used"] is True
    con.close()


def test_stage0_no_match():
    con = _con()
    result = stage0_override(_ctx(uei="UEI_UNKNOWN"), con)
    assert result is None
    con.close()


# ── Stage 1 ───────────────────────────────────────────────────────────────────

def test_stage1_cache_hit():
    con = _con()
    con.execute("""
        INSERT INTO resolution_cache (
            entity_cluster_id, resolved, public_company_id, public_company_name,
            preferred_ticker, preferred_exchange, relationship_type,
            resolution_stage, confidence_score, confidence_band,
            match_explanation, source_evidence_json,
            resolver_version, issuer_master_version,
            first_resolved_at, last_validated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, ["c1", True, "CIK_X", "Acme", "ACME", "Nasdaq", "direct_public_awardee",
          "stage2", 97.0, "very_high", "cached", "{}", "1.0", "unknown",
          "2024-01-01", "2024-01-01"])
    result = stage1_cache(_ctx(), con)
    assert result is not None
    assert result["preferred_ticker"] == "ACME"
    con.close()


def test_stage1_cache_miss():
    con = _con()
    result = stage1_cache(_ctx(), con)
    assert result is None
    con.close()


# ── Stage 2 ───────────────────────────────────────────────────────────────────

def test_stage2_exact_parent_match():
    con = _con()
    _seed_issuer(con, name="Raytheon Technologies", ticker="RTX")
    ctx = _ctx(parent_name="Raytheon Technologies", entity_name="Raytheon Sub")
    result = stage2_exact_parent(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "RTX"
    assert result["relationship_type"] == "public_ultimate_parent"
    assert result["confidence_score"] >= 95
    con.close()


def test_stage2_no_parent_name():
    con = _con()
    _seed_issuer(con)
    result = stage2_exact_parent(_ctx(parent_name=None), con)
    assert result is None
    con.close()


# ── Stage 3 ───────────────────────────────────────────────────────────────────

def test_stage3_direct_match():
    con = _con()
    _seed_issuer(con, name="Palantir Technologies", ticker="PLTR")
    ctx = _ctx(entity_name="Palantir Technologies")
    result = stage3_exact_direct(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "PLTR"
    assert result["relationship_type"] == "direct_public_awardee"
    con.close()


def test_stage3_no_match():
    con = _con()
    _seed_issuer(con, name="Lockheed Martin", ticker="LMT")
    result = stage3_exact_direct(_ctx(entity_name="Foobar Industries"), con)
    assert result is None
    con.close()


# ── Stage 4 ───────────────────────────────────────────────────────────────────

def test_stage4_aggressive_alias_match():
    con = _con()
    _seed_issuer(con, name="Science Applications International", ticker="SAIC")
    ctx = _ctx(entity_name="Science Applications International Corporation")
    result = stage4_alias_match(ctx, con)
    assert result is not None
    assert result["preferred_ticker"] == "SAIC"
    con.close()


# ── Stage 5 ───────────────────────────────────────────────────────────────────

def test_stage5_returns_candidates():
    con = _con()
    _seed_issuer(con, pub_id="CIK_BAH", name="Booz Allen Hamilton", ticker="BAH")
    ctx = _ctx(entity_name="Booz Allen Hamilton Holding")
    candidates = stage5_generate_candidates(ctx, con)
    tickers = [c["ticker_current"] for c in candidates]
    assert "BAH" in tickers
    con.close()


# ── Stage 6 ───────────────────────────────────────────────────────────────────

def test_stage6_scores_token_overlap():
    con = _con()
    _seed_issuer(con, pub_id="CIK_SAIC", name="Science Applications International", ticker="SAIC")
    ctx = _ctx(entity_name="Science Applications International Corporation")
    candidates = stage5_generate_candidates(ctx, con)
    scored = stage6_fuzzy_score(ctx, candidates)
    assert len(scored) > 0
    assert scored[0]["score"] >= 30  # entity-only match (no parent); correct floor
    con.close()


# ── Stage 7 ───────────────────────────────────────────────────────────────────

def test_stage7_accepts_clear_winner():
    thresholds = V1ThresholdsConfig()
    scored = [
        {"public_company_id": "CIK_A", "issuer_name_current": "Acme",
         "ticker_current": "ACME", "exchange_current": "Nasdaq",
         "is_common_equity": True, "score": 91.0, "explanation": "test"},
        {"public_company_id": "CIK_B", "issuer_name_current": "Other",
         "ticker_current": "OTHR", "exchange_current": "NYSE",
         "is_common_equity": True, "score": 55.0, "explanation": "weak"},
    ]
    result = stage7_accept("c1", scored, thresholds)
    assert result["resolved"] is True and result["preferred_ticker"] == "ACME"


def test_stage7_ambiguous_close_scores():
    thresholds = V1ThresholdsConfig()
    scored = [
        {"public_company_id": "CIK_A", "issuer_name_current": "Acme",
         "ticker_current": "ACME", "exchange_current": "Nasdaq",
         "is_common_equity": True, "score": 80.0, "explanation": "x"},
        {"public_company_id": "CIK_B", "issuer_name_current": "Acme2",
         "ticker_current": "ACM2", "exchange_current": "NYSE",
         "is_common_equity": True, "score": 79.0, "explanation": "y"},
    ]
    result = stage7_accept("c1", scored, thresholds)
    assert result["ambiguous"] is True and result["needs_review"] is True


# ── Full orchestrator ─────────────────────────────────────────────────────────

def test_resolve_cluster_uses_cache_on_second_call():
    con = _con()
    _seed_issuer(con, pub_id="CIK_RTX", name="RTX Corporation", ticker="RTX")
    ctx = _ctx(parent_name="RTX Corporation", entity_name="Raytheon Sub")
    r1 = resolve_cluster(ctx, con, V1ThresholdsConfig())
    assert r1["preferred_ticker"] == "RTX"
    r2 = resolve_cluster(ctx, con, V1ThresholdsConfig())
    assert r2["preferred_ticker"] == "RTX"
    assert "cache" in r2["resolution_stage"]
    con.close()
