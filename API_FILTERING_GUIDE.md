# API Response Filtering with jq

This guide provides `jq` filters to minimize API response sizes and save tokens when debugging or testing SAM govArby pipeline API calls.

## Installation

```bash
# macOS
brew install jq

# Windows (via Chocolatey)
choco install jq

# Linux
apt-get install jq
```

## Common Filters

### USASpending.gov API

**Fetch and extract 11 essential fields:**
```bash
curl -s "https://api.usaspending.gov/api/v2/search/spending_by_award/" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{...}' | jq '.results[] | {
    id,
    recipient_name,
    award_amount: .total_dollars_obligated,
    start_date: .period_of_performance_start_date,
    end_date: .period_of_performance_end_date,
    awarding_agency: .awarding_agency_name,
    naics_code,
    contract_type: .type_of_contract_pricing,
    set_aside: .type_of_set_aside,
    sole_source,
    idiq_type
  }'
```

**Output summary (count by agency):**
```bash
cat awards.json | jq '.results[] | {awarding_agency_name} ' | jq -s 'group_by(.awarding_agency_name) | map({agency: .[0].awarding_agency_name, count: length})'
```

### SEC EDGAR API

**Company search – extract only name, CIK, and tickers:**
```bash
curl -s "https://efts.sec.gov/LATEST/search-index/company-search?company_name=ACME" | jq '.hits.hits[] | {
  name: ._source.title,
  cik: ._source.cik,
  tickers: ._source.tickers,
  exchange: ._source.exchange
}'
```

**Submissions API – extract filing metadata:**
```bash
curl -s "https://data.sec.gov/submissions/CIK0000789019.json" | jq '{
  name,
  tickers,
  exchanges,
  entityType,
  recent_filings: .filings.recent | .[] | {accession: .accessionNumber, form: .form, date: .filingDate} | select(.form == "8-K")
}'
```

### yfinance / Yahoo Finance (via curl)

**Get market cap and shares outstanding:**
```bash
# Note: yfinance is Python-only, but you can scrape Yahoo Finance directly
curl -s "https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=price,summaryDetail" | jq '.quoteSummary.result[0] | {
  market_cap: .price.marketCap,
  shares: .summaryDetail.sharesOutstanding,
  currency: .price.currency,
  exchange: .price.exchange
}'
```

### Google News RSS

**Extract article titles and dates:**
```bash
curl -s "https://news.google.com/rss/search?q=small%20cap%20defense&ceid=US:en" | jq -r '.rss.channel.item[] | .title + " | " + .pubDate'
```

**Filter by source (Prnewswire, Businesswire, etc):**
```bash
curl -s "https://news.google.com/rss/search?q=ACME%20contract&ceid=US:en" | jq '.rss.channel.item[] | select(.source | contains("prnewswire") or contains("businesswire")) | {title, link, pubDate}'
```

### SAM.gov Opportunities API

**Extract opportunities by date range:**
```bash
curl -s "https://api.sam.gov/opportunities/v2/search" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{...}' | jq '.opportunitiesData[] | {
    id: .noticeId,
    title: .title,
    agency: .organizationName,
    amount: .estimatedAmount,
    posted: .postedDate,
    deadline: .responseDeadLineDate,
    naics: .naicsCode,
    sole_source: .solicitationNumber
  }'
```

## Piping with RTK

Use `jq` filters with RTK for token-optimized API exploration:

```bash
# Get USASpending response and extract key fields
rtk curl -s "https://api.usaspending.gov/..." | rtk jq '.results[] | {id, recipient_name, award_amount}'

# Count records by agency
rtk curl -s "..." | rtk jq -s 'group_by(.awarding_agency_name) | map({agency: .[0].awarding_agency_name, count: length})'
```

## Practical Examples

### 1. Test API authentication
```bash
curl -s https://api.usaspending.gov/api/v2/search/spending_by_award/ \
  -H "Content-Type: application/json" \
  -d '{"page": 1}' | jq '.error // "OK"'
```

### 2. Count total matches without fetching all data
```bash
curl -s https://api.usaspending.gov/api/v2/search/spending_by_award/ \
  -d '{...}' | jq '.page_metadata.total_matches'
```

### 3. Sample 5 random awards
```bash
curl -s https://api.usaspending.gov/api/v2/search/spending_by_award/ \
  -d '{...}' | jq '.results | .[0:5] | map({awardee: .recipient_name, amount: .total_dollars_obligated})'
```

### 4. Extract and deduplicate agencies
```bash
curl -s https://api.usaspending.gov/api/v2/search/spending_by_award/ \
  -d '{...}' | jq '[.results[].awarding_agency_name] | unique | sort'
```

## Saving API Responses for Debugging

```bash
# Save full response
curl -s "https://api.usaspending.gov/..." > response.json

# Extract and save filtered fields
jq '.results[] | {id, recipient_name, amount}' response.json > filtered.json

# Count records
jq '.results | length' response.json
```

## Token Savings

Using jq to filter API responses **before** passing to Claude:
- **Full response**: ~10,000 tokens (for large API result)
- **jq filtered**: ~500 tokens (50% reduction)
- **Combined with RTK**: 80-90% token savings on API debugging

## References

- **jq Manual**: https://stedolan.github.io/jq/
- **jq Cheat Sheet**: https://stedolan.github.io/jq/manual/
- **SEC EDGAR API**: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
- **USASpending API Docs**: https://api.usaspending.gov/docs/
- **SAM.gov Opportunities API**: https://open.gsa.gov/api/opportunities-api/

## Quick Debugging Tips

1. **Always use `-s` (slurp) with curl** to avoid jq multiline parsing issues
2. **Use `jq 'keys'` to explore JSON structure** when unsure of field names
3. **Pipe `| head` to limit output**: `jq '.[] | first(n)' response.json`
4. **Use `select()` for conditional filtering**: `jq '.[] | select(.award_amount > 1000000)'`
5. **Combine multiple filters**: `jq '.results[] | select(.recipient_name | contains("ACME")) | {id, amount}'`
