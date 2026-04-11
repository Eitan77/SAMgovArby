"""tests/resolver/test_storage.py"""
import os, tempfile, pytest
from resolver.storage import get_db, ensure_schema, TABLES


def test_ensure_schema_creates_all_tables():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "test.duckdb"))
        ensure_schema(con)
        existing = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for t in TABLES:
            assert t in existing, f"Missing table: {t}"
        con.close()


def test_uniqueness_contract_row_id():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        con.execute("INSERT INTO contracts_normalized (contract_row_id) VALUES ('r1')")
        with pytest.raises(Exception):
            con.execute("INSERT INTO contracts_normalized (contract_row_id) VALUES ('r1')")
        con.close()


def test_issuer_master_unique_on_pub_id():
    with tempfile.TemporaryDirectory() as d:
        con = get_db(os.path.join(d, "t.duckdb"))
        ensure_schema(con)
        con.execute("INSERT INTO issuer_master (public_company_id) VALUES ('CIK_001')")
        with pytest.raises(Exception):
            con.execute("INSERT INTO issuer_master (public_company_id) VALUES ('CIK_001')")
        con.close()
