"""resolver/cli.py — Batch entry point: python -m resolver [args]."""
from __future__ import annotations
import argparse, json, logging, os, uuid
from datetime import datetime
from pathlib import Path
import pandas as pd
from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import refresh_issuer_master, get_issuer_master_version
from resolver.clusters import build_entity_clusters
from resolver.pipeline import ClusterContext, resolve_cluster
from resolver.models import V1ThresholdsConfig, RESOLVER_V1_VERSION
from resolver.normalize import conservative_normalize
from resolver.ingest import load_contracts, assign_contract_row_ids, build_contract_identity_features

log = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resolver V1 batch pipeline")
    p.add_argument("--input",       required=True, nargs="+",
                   help="Contract CSV or Parquet file(s)")
    p.add_argument("--output-dir",  default="data/outputs")
    p.add_argument("--db",          default="data/cache/resolver.duckdb")
    p.add_argument("--cache-dir",   default="data/cache")
    p.add_argument("--config",      default=None,
                   help="JSON config file with threshold overrides")
    p.add_argument("--refresh",     action="store_true",
                   help="Force refresh issuer master from SEC + Nasdaq")
    p.add_argument("--no-refresh",  action="store_true",
                   help="Skip issuer master refresh (use existing DB)")
    p.add_argument("--openfigi-tail", action="store_true",
                   help="Enable Stage 8 OpenFIGI tail enrichment")
    p.add_argument("--enrich-edgar", action="store_true", default=True,
                   help="Fetch EDGAR former names for all CIK issuers (cached, default on)")
    p.add_argument("--no-enrich-edgar", action="store_false", dest="enrich_edgar",
                   help="Skip EDGAR former name enrichment")
    p.add_argument("--log-level",   default="INFO")
    return p


def load_thresholds(config_path: str | None) -> V1ThresholdsConfig:
    t = V1ThresholdsConfig()
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            overrides = json.load(f).get("thresholds", {})
        for k, v in overrides.items():
            if hasattr(t, k):
                setattr(t, k, v)
    return t


def run_batch(args) -> None:
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    run_id  = str(uuid.uuid4())[:8]
    started = datetime.utcnow()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    con = get_db(args.db)
    ensure_schema(con)

    # Issuer master refresh decision
    enrich = getattr(args, "enrich_edgar", True)
    if args.refresh:
        im_version = refresh_issuer_master(con, args.cache_dir, force=True, enrich_edgar=enrich)
    elif args.no_refresh:
        im_version = get_issuer_master_version(con)
    else:
        im_version = refresh_issuer_master(con, args.cache_dir, force=False, enrich_edgar=enrich)

    thresholds = load_thresholds(args.config)
    if args.openfigi_tail:
        thresholds.enable_openfigi_tail = True

    # Ingest
    all_dfs = []
    for path in args.input:
        if path.endswith(".parquet"):
            all_dfs.append(pd.read_parquet(path))
        else:
            all_dfs.append(pd.read_csv(path, low_memory=False))
    df = pd.concat(all_dfs, ignore_index=True)
    df = assign_contract_row_ids(df)
    features = build_contract_identity_features(df)
    log.info(f"Loaded {len(df)} rows → {len(features)} features")

    # Cluster
    row_to_cluster = build_entity_clusters(features, con)
    feat_map = {f.contract_row_id: f for f in features}

    # Fetch clusters
    clusters = con.execute(
        "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
        "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
        "all_legal_names_json, all_dba_names_json FROM entity_clusters"
    ).fetchall()

    metrics = {
        "stage_wins": {}, "new_resolved": 0, "ambiguous_count": 0,
        "unresolved_count": 0, "override_hits": 0, "cache_hits": 0,
    }

    cluster_results: dict[str, dict] = {}
    for row in clusters:
        cid, cp, ce, puei, uei, cage, pj, lj, dj = row
        ctx = ClusterContext(
            cluster_id=cid,
            canonical_parent_name=cp,
            canonical_entity_name=ce,
            uei=uei, parent_uei=puei, cage=cage,
            parent_name_norm=conservative_normalize(cp),
            legal_name_norm=conservative_normalize(ce),
            all_parent_names=json.loads(pj or "[]"),
            all_legal_names=json.loads(lj or "[]"),
            all_dba_names=json.loads(dj or "[]"),
        )
        result = resolve_cluster(ctx, con, thresholds, im_version)
        cluster_results[cid] = result

        stage = result.get("resolution_stage", "unresolved")
        metrics["stage_wins"][stage] = metrics["stage_wins"].get(stage, 0) + 1
        if result.get("resolved"):
            metrics["new_resolved"] += 1
        if result.get("ambiguous"):
            metrics["ambiguous_count"] += 1
        if not result.get("resolved") and not result.get("ambiguous"):
            metrics["unresolved_count"] += 1
        if "cache" in stage:
            metrics["cache_hits"] += 1
        if result.get("manual_override_used"):
            metrics["override_hits"] += 1

    # Fan back to rows
    rows_out = []
    for _, row in df.iterrows():
        cid = row_to_cluster.get(row.get("contract_row_id", ""), "")
        r   = dict(row)
        r.update(cluster_results.get(cid, {}))
        rows_out.append(r)

    # Write resolution_results CSV
    result_df = pd.DataFrame(rows_out)
    out_path  = os.path.join(args.output_dir, f"resolution_results_{run_id}.csv")
    result_df.to_csv(out_path, index=False)
    log.info(f"Results: {out_path}")

    # Write review queue
    ambiguous = [r for r in cluster_results.values() if r.get("needs_review")]
    if ambiguous:
        queue_path = os.path.join(args.output_dir, f"review_queue_{run_id}.csv")
        pd.DataFrame(ambiguous).to_csv(queue_path, index=False)

    # Run log to DuckDB
    ended = datetime.utcnow()
    con.execute("""
        INSERT OR REPLACE INTO resolver_run_log (
            run_id, started_at, ended_at, total_rows, total_clusters,
            new_resolved, stage_wins_json, unresolved_count, ambiguous_count,
            override_hits, cache_hits, avg_candidates_scored,
            resolver_version, config_hash
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [run_id, started.isoformat(), ended.isoformat(),
          len(df), len(clusters),
          metrics["new_resolved"], json.dumps(metrics["stage_wins"]),
          metrics["unresolved_count"], metrics["ambiguous_count"],
          metrics["override_hits"], metrics["cache_hits"],
          0.0, RESOLVER_V1_VERSION, ""])

    total = len(clusters)
    resolved = metrics["new_resolved"]
    rate = resolved / total * 100 if total else 0
    print(f"\n{'='*55}")
    print(f"Resolver V1 — Run {run_id}")
    print(f"  Rows:       {len(df):,}")
    print(f"  Clusters:   {total:,}")
    print(f"  Resolved:   {resolved:,}  ({rate:.1f}%)")
    print(f"  Ambiguous:  {metrics['ambiguous_count']:,}")
    print(f"  Unresolved: {metrics['unresolved_count']:,}")
    print(f"  Cache hits: {metrics['cache_hits']:,}")
    print(f"  Stage wins: {json.dumps(metrics['stage_wins'])}")
    print(f"{'='*55}\n")
