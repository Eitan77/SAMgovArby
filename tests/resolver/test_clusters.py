"""tests/resolver/test_clusters.py"""
import os, tempfile
from decimal import Decimal
from datetime import date
from resolver.storage import get_db, ensure_schema
from resolver.clusters import build_entity_clusters, choose_cluster_key
from resolver.models import ContractIdentityFeatures


def _feat(row_id, uei=None, parent_uei=None, cage=None, name=None, parent_name=None, dba=None):
    return ContractIdentityFeatures(
        contract_row_id=row_id, award_date=date(2023, 1, 1),
        awardee_uei=uei, parent_uei=parent_uei, cage_code=cage,
        awardee_name_raw=name, awardee_name_norm=name,
        awardee_dba_raw=dba, awardee_dba_norm=dba,
        parent_name_raw=parent_name, parent_name_norm=parent_name,
        website_raw=None, website_domain=None,
        vendor_city_norm=None, vendor_state_norm="VA",
        vendor_zip_norm=None, vendor_country_norm="USA",
        incorporation_country_norm="USA", phone_norm=None,
        dollars_obligated=Decimal("1000000"),
    )


def test_parent_uei_wins_over_uei():
    f = _feat("r1", uei="U1", parent_uei="PUEI_X")
    ktype, kval = choose_cluster_key(f)
    assert ktype == "ultimate_parent_uei" and kval == "PUEI_X"


def test_cage_used_when_no_uei():
    f = _feat("r1", cage="12345", name="Some Corp")
    ktype, kval = choose_cluster_key(f)
    assert ktype == "cage" and kval == "12345"


def test_same_parent_uei_clusters_together():
    feats = [
        _feat("r1", parent_uei="PUEI_X", name="Sub A", parent_name="Parent Co"),
        _feat("r2", parent_uei="PUEI_X", name="Sub B", parent_name="Parent Co"),
        _feat("r3", parent_uei="PUEI_Y", name="Other"),
    ]
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        build_entity_clusters(feats, con)
        clusters = con.execute(
            "SELECT row_count FROM entity_clusters ORDER BY row_count DESC"
        ).fetchall()
        assert len(clusters) == 2
        assert clusters[0][0] == 2
        assert clusters[1][0] == 1
        con.close()


def test_canonical_parent_name_stored():
    feats = [_feat("r1", parent_uei="PUEI_X", parent_name="RTX Corporation")]
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        build_entity_clusters(feats, con)
        row = con.execute("SELECT canonical_parent_name FROM entity_clusters").fetchone()
        assert row[0] == "RTX Corporation"
        con.close()
