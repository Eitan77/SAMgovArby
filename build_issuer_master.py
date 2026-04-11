"""One-shot issuer master rebuild. Run: python build_issuer_master.py"""
import duckdb, json, os, sys, time, logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
sys.path.insert(0, os.path.dirname(__file__))

from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import build_issuer_master_from_fixtures

cache = 'data/cache'
for f in [f'{cache}/resolver.duckdb', f'{cache}/resolver.duckdb.wal']:
    try: os.remove(f)
    except: pass

tickers  = json.loads(open(f'{cache}/2ba855dd3ccb016d4a192be339a66e36.json').read())
exchange = json.loads(open(f'{cache}/4539a807ba91f0f7e53d8f3f62d4cf3a.json').read())
nasdaq   = open(f'{cache}/124571c9c194c049d610b651c0f767f1.txt').read()
other    = open(f'{cache}/fe58781afdc79b02fe4b41d4d40ebfa9.txt').read()

con = get_db(f'{cache}/resolver.duckdb')
ensure_schema(con)
t0 = time.time()
build_issuer_master_from_fixtures(con, tickers, exchange, nasdaq, other)
elapsed = time.time() - t0

n = con.execute('SELECT COUNT(*) FROM issuer_master').fetchone()[0]
c = con.execute('SELECT COUNT(*) FROM issuer_master WHERE is_common_equity=TRUE').fetchone()[0]
a = con.execute('SELECT COUNT(*) FROM issuer_aliases').fetchone()[0]
adm = con.execute("SELECT alias_normalized_aggressive FROM issuer_aliases WHERE public_company_id='CIK_0000007084'").fetchall()
ut  = con.execute("SELECT issuer_name_current, is_common_equity, ticker_current FROM issuer_master WHERE issuer_name_current LIKE '%United Tech%'").fetchall()
con.close()

print(f'BUILD OK in {elapsed:.1f}s: master={n} common={c} aliases={a}')
print(f'ADM aliases: {adm}')
print(f'UTX: {ut}')
