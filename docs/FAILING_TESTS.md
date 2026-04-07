# Pre-Existing Test Failures

Both failures existed in the repository before the implementation plan was applied (confirmed by `git stash` + rerun). Neither was introduced by recent changes.

---

## 1. `tests/test_sam_gov_reader.py::test_valid_row_yields_contract_record`

### Failure

```
AssertionError: assert '' == '2023-03-15'
```

### Root cause

The test builds a CSV row using `_base_row()`, which sets the date under the column `"Period of Performance Start Date"`:

```python
"Period of Performance Start Date": "2023-03-15",
```

However, `sam_gov_reader.py` reads `posted_date` from a **different column**:

```python
posted_date = _parse_date(row.get("Date Signed") or "")
```

The column `"Date Signed"` is absent from `_base_row()`, so `row.get("Date Signed")` returns `None`, `_parse_date("")` returns `""`, and `r.posted_date` is always `""`.

The test was written against an older version of `sam_gov_reader.py` that mapped `posted_date` to `"Period of Performance Start Date"`. The reader was later changed to use `"Date Signed"` (commit `c1ac57c`) but the test fixture was not updated.

### Fix required

Add `"Date Signed": "2023-03-15"` to `_base_row()` in `tests/test_sam_gov_reader.py` (and optionally remove or keep the now-unused `"Period of Performance Start Date"` key):

```python
"Date Signed": "2023-03-15",
```

---

## 2. `tests/test_ticker_resolver_v4.py::test_tier1_cage_resolves_to_ticker`

### Failure

```
AssertionError: assert None == 'ACME'
```

### Root cause

The test mocks `r.cage_resolver` and `r.lei_resolver` and expects Tier 1 resolution to go through those mocks:

```python
r.cage_resolver = cage_mock   # expects resolve_cage("1RBX4") to be called
r.lei_resolver = lei_mock     # expects resolve_lei(lei) to be called
r.mcap_cache["ACME"] = 500_000_000.0
```

It then asserts:
```python
assert result["resolved_ticker"] == "ACME"
assert result["evidence_type"] == "cage_lei_openfigi"
cage_mock.resolve_cage.assert_called_once_with("1RBX4")
```

However, the **current** `_resolve_via_cage()` implementation (added in commit `87b89d5`) does **not** call `self.cage_resolver.resolve_cage()`. Instead it calls the GLEIF REST API directly via `requests.get("https://leilookup.gleif.org/api/v3/lei-records", ...)`, iterates the returned LEI records, and calls `self.lei_resolver.resolve_lei()` only if a record comes back from the network.

In the test environment there is no network (or the GLEIF API is not reachable), so the HTTP call fails or returns no records, Tier 1 returns `{}`, and resolution falls through all tiers to `None`.

The mocks are never invoked because the code path that would call them no longer exists.

### Fix required

The test needs to be rewritten to match the current implementation. The GLEIF `requests.get` call should be patched at the network level rather than at the `cage_resolver` attribute level:

```python
@patch("ticker_resolver_v4.requests.get")
def test_tier1_cage_resolves_to_ticker(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "lei_records": [{"lei": "ABCDE12345FGHIJ67890"}]
    }
    lei_mock = MagicMock()
    lei_mock.resolve_lei.return_value = {
        "ticker": "ACME",
        "cik": "0001234567",
        "confidence": "high",
    }
    r = make_resolver()
    r.lei_resolver = lei_mock
    r.mcap_cache["ACME"] = 500_000_000.0

    result = r.resolve(make_record(cage_code="1RBX4"))

    assert result["resolved_ticker"] == "ACME"
    assert result["confidence"] == "high"
    assert result["evidence_type"] == "cage_gleif_lei_openfigi"
```

Note the evidence type also changed from `"cage_lei_openfigi"` to `"cage_gleif_lei_openfigi"` in the current implementation.
