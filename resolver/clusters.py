"""resolver/clusters.py — Build entity_clusters from contract identity features."""
from __future__ import annotations
import hashlib, json, logging
from collections import Counter, defaultdict
import duckdb
import pandas as pd
from resolver.models import ContractIdentityFeatures

log = logging.getLogger(__name__)


def choose_cluster_key(feat: ContractIdentityFeatures) -> tuple[str, str]:
    """Spec priority: parent_uei > uei > cage > name_sig."""
    if feat.parent_uei:
        return "ultimate_parent_uei", feat.parent_uei
    if feat.awardee_uei:
        return "uei", feat.awardee_uei
    if feat.cage_code:
        return "cage", feat.cage_code
    base = feat.parent_name_norm or feat.awardee_name_norm or feat.contract_row_id
    return "name_sig", hashlib.md5(base.encode()).hexdigest()[:12]


def _cluster_id(key_type: str, key_value: str) -> str:
    return hashlib.md5(f"{key_type}:{key_value}".encode()).hexdigest()[:16]


def _most_common(vals: list) -> str | None:
    if not vals:
        return None
    return Counter(vals).most_common(1)[0][0]


def build_entity_clusters(
    features: list[ContractIdentityFeatures],
    con: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    """
    Cluster features into entity_clusters in DuckDB.
    Returns {contract_row_id: entity_cluster_id}.
    """
    buckets: dict[str, list[ContractIdentityFeatures]] = defaultdict(list)
    row_map: dict[str, str] = {}

    for feat in features:
        ktype, kval = choose_cluster_key(feat)
        cid = _cluster_id(ktype, kval)
        buckets[cid].append(feat)
        row_map[feat.contract_row_id] = cid

    cluster_rows: list[tuple] = []
    edge_rows:    list[tuple] = []

    for cid, feats in buckets.items():
        ktype, kval = choose_cluster_key(feats[0])
        all_ueis    = list({f.awardee_uei       for f in feats if f.awardee_uei})
        all_cages   = list({f.cage_code         for f in feats if f.cage_code})
        all_legal   = list({f.awardee_name_raw  for f in feats if f.awardee_name_raw})
        all_parents = list({f.parent_name_raw   for f in feats if f.parent_name_raw})
        all_dbas    = list({f.awardee_dba_raw   for f in feats if f.awardee_dba_raw})
        state_freq  = dict(Counter(f.vendor_state_norm  for f in feats if f.vendor_state_norm))
        ctry_freq   = dict(Counter(f.vendor_country_norm for f in feats if f.vendor_country_norm))
        dates       = sorted(f.award_date for f in feats if f.award_date)
        total_obl   = sum(float(f.dollars_obligated or 0) for f in feats)

        can_parent  = _most_common(all_parents)
        can_entity  = _most_common(all_legal)

        parent_uei  = next((f.parent_uei  for f in feats if f.parent_uei),  None)
        uei         = next((f.awardee_uei for f in feats if f.awardee_uei), None)
        cage        = next((f.cage_code   for f in feats if f.cage_code),   None)

        cluster_rows.append((
            cid, ktype,
            parent_uei, uei, cage,
            can_entity, can_parent, can_parent or can_entity,
            json.dumps(all_ueis), json.dumps(all_cages),
            json.dumps(all_legal), json.dumps(all_parents), json.dumps(all_dbas),
            json.dumps(state_freq), json.dumps(ctry_freq), "{}",
            str(dates[0]) if dates else None,
            str(dates[-1]) if dates else None,
            len(feats), total_obl,
        ))

        for feat in feats:
            eid = hashlib.md5(f"{feat.contract_row_id}:{cid}".encode()).hexdigest()[:16]
            edge_rows.append((eid, cid, feat.contract_row_id, ktype))

    # Bulk insert via pandas → DuckDB (single round-trip each)
    if cluster_rows:
        cdf = pd.DataFrame(cluster_rows, columns=[
            "entity_cluster_id", "cluster_key_type",
            "ultimate_parent_uei", "uei", "cage_code",
            "canonical_entity_name", "canonical_parent_name", "canonical_display_name",
            "all_ueis_json", "all_cages_json", "all_legal_names_json",
            "all_parent_names_json", "all_dba_names_json",
            "state_freq_json", "country_freq_json", "naics_freq_json",
            "first_seen_date", "last_seen_date", "row_count", "total_obligated",
        ])
        con.register("_clusters_tmp", cdf)
        con.execute("INSERT OR REPLACE INTO entity_clusters SELECT * FROM _clusters_tmp")
        con.unregister("_clusters_tmp")

    if edge_rows:
        edf = pd.DataFrame(edge_rows, columns=[
            "edge_id", "entity_cluster_id", "contract_row_id", "edge_type",
        ])
        con.register("_edges_tmp", edf)
        con.execute("INSERT OR IGNORE INTO entity_edges SELECT * FROM _edges_tmp")
        con.unregister("_edges_tmp")

    log.info(f"Built {len(buckets)} clusters from {len(features)} rows")
    return row_map
