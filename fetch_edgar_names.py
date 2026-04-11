"""Fetch EDGAR former names for all CIK issuers. Runs cached.
Run: python fetch_edgar_names.py
This will take ~15 minutes on first run; all results cached permanently."""
import sys, os, logging, time
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
sys.path.insert(0, os.path.dirname(__file__))

from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import enrich_with_edgar_former_names

con = get_db('data/cache/resolver.duckdb')
ensure_schema(con)
t0 = time.time()
added = enrich_with_edgar_former_names(con, 'data/cache', max_ciks=0)
elapsed = time.time() - t0
n = con.execute('SELECT COUNT(*) FROM issuer_aliases').fetchone()[0]
print(f'Added {added} former-name aliases in {elapsed:.0f}s. Total aliases: {n}')
con.close()
