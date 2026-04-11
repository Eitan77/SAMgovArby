"""Benchmark on stage2 dataset (has known tickers for comparison).
Run: python run_resolver_bench.py"""
import json, logging, time, sys, os
logging.basicConfig(level=logging.WARNING)  # suppress info for speed
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from resolver.storage import get_db, ensure_schema
from resolver.clusters import build_entity_clusters
from resolver.pipeline import (ClusterContext, resolve_cluster,
                               flush_resolution_cache_pending,
                               invalidate_resolution_cache)
from resolver.models import V1ThresholdsConfig
from resolver.normalize import conservative_normalize
from resolver.ingest import assign_contract_row_ids, build_contract_identity_features

# Use 1000 rows from the filtered training set for speed
df = pd.read_csv('datasets/stage2_with_tickers_H1.csv', nrows=2000)
print(f'Input: {len(df)} rows from stage2_with_tickers_H1')
known_rows = df[df['ticker'].notna() & (df['ticker'] != '')]
print(f'Known resolved by old resolver: {len(known_rows)} ({len(known_rows)/len(df)*100:.1f}%)')

df = assign_contract_row_ids(df)
features = build_contract_identity_features(df)

con = get_db('data/cache/resolver.duckdb')
# Clear old clusters from test run
con.execute("DELETE FROM entity_clusters")
con.execute("DELETE FROM resolution_cache")
invalidate_resolution_cache()  # flush in-memory cache after DELETE

t0 = time.time()
row_to_cluster = build_entity_clusters(features, con)
clusters = con.execute(
    "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
    "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
    "all_legal_names_json, all_dba_names_json FROM entity_clusters"
).fetchall()
print(f'Clusters: {len(clusters)} in {time.time()-t0:.1f}s')

# Build ground truth: cluster_id → known_ticker (from old resolver)
cluster_to_known: dict[str, str] = {}
df_with_ids = df.copy()
df_with_ids['_cluster_id'] = df_with_ids['contract_row_id'].map(row_to_cluster)
for _, row in df_with_ids.iterrows():
    t = row.get('ticker', '')
    cid = row.get('_cluster_id', '')
    if t and cid and pd.notna(t) and pd.notna(cid):
        cluster_to_known[cid] = str(t).strip().upper()

print(f'Clusters with known ticker: {len(cluster_to_known)}')

thresholds = V1ThresholdsConfig()
stage_wins = {}
resolved = ambig = unresolved = 0
resolved_tickers = set()

# Accuracy tracking
tp = fp = fn = 0  # true positive, false positive, false negative

t1 = time.time()
for row in clusters:
    cid, cp, ce, puei, uei, cage, pj, lj, dj = row
    ctx = ClusterContext(
        cluster_id=cid,
        canonical_parent_name=cp,
        canonical_entity_name=ce,
        uei=uei, parent_uei=puei, cage=cage,
        parent_name_norm=conservative_normalize(cp),
        legal_name_norm=conservative_normalize(ce),
        all_parent_names=json.loads(pj or '[]'),
        all_legal_names=json.loads(lj or '[]'),
        all_dba_names=json.loads(dj or '[]'),
    )
    result = resolve_cluster(ctx, con, thresholds, 'bench')
    stage = result.get('resolution_stage', 'unresolved')
    stage_wins[stage] = stage_wins.get(stage, 0) + 1
    if result.get('resolved'):
        resolved += 1
        t = (result.get('preferred_ticker', '') or '').upper()
        if t:
            resolved_tickers.add(t)
        known = cluster_to_known.get(cid, '')
        if known:
            if t == known:
                tp += 1
            else:
                fp += 1
        # no 'known' means new resolution (no ground truth — don't penalize)
    elif result.get('ambiguous'):
        ambig += 1
        if cid in cluster_to_known:
            fn += 1  # had ground truth but couldn't resolve
    else:
        unresolved += 1
        if cid in cluster_to_known:
            fn += 1  # had ground truth but couldn't resolve

flush_resolution_cache_pending(con)  # flush queued writes in one bulk op
elapsed = time.time() - t1
total = len(clusters)
print(f'\nResolution in {elapsed:.1f}s ({elapsed/total*1000:.0f}ms/cluster)')
print(f'Total: {total}  Resolved: {resolved} ({resolved/total*100:.1f}%)  Ambig: {ambig}  Unresolved: {unresolved}')

known_total = len(cluster_to_known)
precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall    = tp / known_total if known_total > 0 else 0.0
f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
print(f'\nVs old resolver ground truth ({known_total} clusters with known tickers):')
print(f'  TP={tp}  FP={fp}  FN={fn}')
print(f'  Precision={precision:.1%}  Recall={recall:.1%}  F1={f1:.1%}')
print(f'Stage wins: {json.dumps(dict(sorted(stage_wins.items(), key=lambda x: -x[1])), indent=2)}')
con.close()
