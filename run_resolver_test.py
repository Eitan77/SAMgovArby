"""Quick resolution test on test_week.csv. Run: python run_resolver_test.py"""
import json, logging, time, sys, os
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from resolver.storage import get_db, ensure_schema
from resolver.clusters import build_entity_clusters
from resolver.pipeline import ClusterContext, resolve_cluster
from resolver.models import V1ThresholdsConfig
from resolver.normalize import conservative_normalize
from resolver.ingest import assign_contract_row_ids, build_contract_identity_features

df = pd.read_csv('datasets/test_week.csv')
print(f'Input: {len(df)} rows')
df = assign_contract_row_ids(df)
features = build_contract_identity_features(df)
print(f'Features extracted, sample UEI: {features[0].awardee_uei}, parent: {features[0].parent_name_raw}')

con = get_db('data/cache/resolver.duckdb')
ensure_schema(con)

t0 = time.time()
row_to_cluster = build_entity_clusters(features, con)
clusters = con.execute(
    "SELECT entity_cluster_id, canonical_parent_name, canonical_entity_name, "
    "ultimate_parent_uei, uei, cage_code, all_parent_names_json, "
    "all_legal_names_json, all_dba_names_json FROM entity_clusters"
).fetchall()
print(f'Clusters: {len(clusters)} in {time.time()-t0:.1f}s')

thresholds = V1ThresholdsConfig()
stage_wins = {}
resolved = ambig = unresolved = 0
sample_resolved = []

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
    result = resolve_cluster(ctx, con, thresholds, 'test')
    stage = result.get('resolution_stage', 'unresolved')
    stage_wins[stage] = stage_wins.get(stage, 0) + 1
    if result.get('resolved'):
        resolved += 1
        sample_resolved.append((ce or cp, result['preferred_ticker'], result['confidence_score'], stage))
    elif result.get('ambiguous'):
        ambig += 1
    else:
        unresolved += 1

elapsed = time.time() - t1
total = len(clusters)
print(f'\nResolution in {elapsed:.1f}s ({elapsed/total*1000:.0f}ms/cluster)')
print(f'Total: {total}  Resolved: {resolved} ({resolved/total*100:.1f}%)  Ambig: {ambig}  Unresolved: {unresolved}')
print(f'Stage wins: {json.dumps(stage_wins, indent=2)}')
print(f'\nSample resolved:')
for name, ticker, score, stage in sample_resolved[:20]:
    print(f'  {name[:40]:40s} -> {ticker:6s} ({score:.0f}) [{stage}]')
con.close()
