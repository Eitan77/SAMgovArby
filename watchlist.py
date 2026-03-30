"""Known publicly-traded small/mid-cap defense & government tech companies.

These are the universe of stocks the strategy targets. The backtest will:
1. Query USASpending for contracts awarded to these specific companies
2. Simulate trade outcomes for each contract award

Market caps as of early 2023 (approximate). Update periodically.
Focused on companies < ~$2B market cap that regularly win government contracts.
"""

# (ticker, primary_name, aliases)
# aliases = other names the company may appear as in SAM.gov/USASpending
WATCHLIST = [
    # Aerospace & Defense - small cap
    ("AVAV",  "AeroVironment",           ["aerovironment"]),
    ("CODA",  "CODA Octopus",            ["coda octopus"]),
    ("CVU",   "CPI Aerostructures",      ["cpi aerostructures"]),
    ("DLHC",  "DLH Holdings",            ["dlh holdings", "biomedical systems"]),
    ("GFAI",  "Guardforce AI",           ["guardforce"]),
    ("HAYW",  "Haynes International",    ["haynes international"]),
    ("HII",   "Huntington Ingalls",      ["huntington ingalls", "newport news"]),
    ("KTOS",  "Kratos Defense",          ["kratos defense", "kratos unmanned"]),
    ("LTBR",  "Lightbridge",             ["lightbridge"]),
    ("MFAC",  "Methode Electronics",     ["methode electronics"]),
    ("MRLN",  "Marlin Business",         ["marlin business"]),
    ("OSS",   "One Stop Systems",        ["one stop systems"]),
    ("OSIS",  "OSI Systems",             ["osi systems", "rapiscan"]),
    ("PLAV",  "Palav",                   ["palav"]),
    ("RCAT",  "Red Cat Holdings",        ["red cat"]),
    ("SPIR",  "Spire Global",            ["spire global"]),
    ("SWBI",  "Smith & Wesson",          ["american outdoor brands", "smith wesson"]),
    ("TITN",  "Titan Machinery",         ["titan machinery"]),
    ("TDW",   "Tidewater",               ["tidewater"]),
    ("TPVG",  "TriplePoint Venture",     ["triplepoint"]),
    ("VEC",   "Vectrus",                 ["vectrus"]),
    ("VSEC",  "VSE Corporation",         ["vse corporation", "vse corp"]),
    ("WLDN",  "Willdan Group",           ["willdan"]),
    # IT / Cybersecurity government services - small cap
    ("CACI",  "CACI International",      ["caci international", "caci"]),
    ("ICAD",  "iCAD",                    ["icad"]),
    ("KEYW",  "KeyW Holding",            ["keyw"]),
    ("LDOS",  "Leidos",                  ["leidos"]),
    ("MAN",   "ManTech International",   ["mantech"]),
    ("MANT",  "ManTech",                 ["mantech international"]),
    ("MAXR",  "Maxar Technologies",      ["maxar"]),
    ("PAE",   "PAE",                     ["pae incorporated", "pae inc"]),
    ("PLXS",  "Plexus",                  ["plexus corp"]),
    ("SAIC",  "Science Applications",    ["science applications", "saic"]),
    ("SMTC",  "Semtech",                 ["semtech"]),
    # Engineering & technical services
    ("ALGT",  "Allegiant Travel",        ["allegiant"]),
    ("AMRK",  "A-Mark Precious",         ["a-mark"]),
    ("DSGX",  "Descartes Systems",       ["descartes"]),
    ("EML",   "Eastern Company",         ["eastern company"]),
    ("FLIR",  "FLIR Systems",            ["flir systems"]),
    ("GEO",   "GEO Group",               ["geo group"]),
    ("HWM",   "Howmet Aerospace",        ["howmet"]),
    ("ISSC",  "Innovative Solutions",    ["innovative solutions support"]),
    ("LDSS",  "LDSS Inc",                ["ldss"]),
    ("NRC",   "NRC Group",               ["nrc group"]),
    ("NVEE",  "NV5 Global",              ["nv5"]),
    ("TGI",   "Triumph Group",           ["triumph group"]),
    ("VSAT",  "Viasat",                  ["viasat"]),
]


def get_ticker_to_names():
    """Return dict: ticker -> [name, *aliases]."""
    result = {}
    for ticker, name, aliases in WATCHLIST:
        result[ticker] = [name.lower()] + [a.lower() for a in aliases]
    return result


def get_all_search_names():
    """Return list of (ticker, search_name) pairs for USASpending queries."""
    pairs = []
    for ticker, name, aliases in WATCHLIST:
        pairs.append((ticker, name))
        for alias in aliases:
            pairs.append((ticker, alias))
    return pairs
