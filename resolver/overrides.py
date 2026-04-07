"""resolver/overrides.py — Manual override manager."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from resolver.models import EntityResolutionDecision, OverrideRecord, ResolverConfig
from resolver.normalize import parse_date

log = logging.getLogger(__name__)

# ── Manager ───────────────────────────────────────────────────────────────────

def load_overrides(config: ResolverConfig) -> dict[str, OverrideRecord]:
    """Load all overrides from SQLite → dict[entity_key, OverrideRecord]."""
    from resolver.persistence import load_overrides_from_db
    rows    = load_overrides_from_db(config)
    result  = {}
    for row in rows:
        try:
            rec = OverrideRecord(
                entity_key     = row["entity_key"],
                fixed_issuer   = row.get("fixed_issuer"),
                fixed_ticker   = row.get("fixed_ticker"),
                forced_null    = bool(row.get("forced_null", 0)),
                award_date_from = parse_date(row.get("award_date_from")),
                award_date_to   = parse_date(row.get("award_date_to")),
                reason         = row.get("reason"),
                reviewer       = row.get("reviewer"),
                created_at     = datetime.fromisoformat(row["created_at"])
                                 if row.get("created_at") else datetime.utcnow(),
            )
            result[rec.entity_key] = rec
        except Exception as e:
            log.warning(f"Skipping bad override row: {e}")
    return result

def apply_overrides_to_entity(
    entity_key:  str,
    award_date:  date | None,
    overrides:   dict[str, OverrideRecord],
) -> EntityResolutionDecision | None:
    """
    Check if an override applies. Returns an EntityResolutionDecision if it does,
    or None if no override applies.
    """
    rec = overrides.get(entity_key)
    if not rec:
        return None

    # Date range check
    if rec.award_date_from and award_date and award_date < rec.award_date_from:
        return None
    if rec.award_date_to and award_date and award_date > rec.award_date_to:
        return None

    if rec.forced_null:
        return EntityResolutionDecision(
            entity_key       = entity_key,
            entity_type      = "awardee",
            decision_status  = "no_match",
            matched_issuer_key   = None,
            matched_issuer_name  = None,
            matched_cik          = None,
            matched_lei          = None,
            top_score            = 0.0,
            resolution_path      = "override_forced_null",
            evidence_json        = {"reason": rec.reason, "reviewer": rec.reviewer},
        )

    if rec.fixed_ticker or rec.fixed_issuer:
        return EntityResolutionDecision(
            entity_key       = entity_key,
            entity_type      = "awardee",
            decision_status  = "resolved_issuer",
            matched_issuer_key   = f"override:{rec.fixed_issuer or rec.fixed_ticker}",
            matched_issuer_name  = rec.fixed_issuer or "",
            matched_cik          = None,
            matched_lei          = None,
            top_score            = 100.0,
            resolution_path      = "manual_override",
            evidence_json        = {
                "fixed_ticker": rec.fixed_ticker,
                "fixed_issuer": rec.fixed_issuer,
                "reason":       rec.reason,
                "reviewer":     rec.reviewer,
            },
        )
    return None

def apply_overrides_to_contract(
    contract_row_id: str,
    entity_key: str | None,
    award_date: date | None,
    overrides: dict[str, OverrideRecord],
) -> dict | None:
    """Check overrides by entity_key for a contract row."""
    if not entity_key:
        return None
    decision = apply_overrides_to_entity(entity_key, award_date, overrides)
    if decision is None:
        return None
    return {
        "contract_row_id":               contract_row_id,
        "override_decision":             decision,
        "resolver_manual_override_applied": True,
    }

def record_override(entity_key: str, override_payload: dict, config: ResolverConfig) -> None:
    """Persist a new override to SQLite."""
    from resolver.persistence import save_override_to_db
    validated = validate_override_payload(override_payload)
    validated["entity_key"] = entity_key
    validated["created_at"] = datetime.utcnow().isoformat()
    save_override_to_db(validated, config)

def validate_override_payload(payload: dict) -> dict:
    """Check override payload for required fields. Returns cleaned payload."""
    allowed_keys = {
        "entity_key", "fixed_issuer", "fixed_ticker", "forced_null",
        "award_date_from", "award_date_to", "reason", "reviewer", "created_at",
    }
    cleaned = {k: v for k, v in payload.items() if k in allowed_keys}
    if not cleaned.get("forced_null") and not cleaned.get("fixed_ticker") and not cleaned.get("fixed_issuer"):
        raise ValueError("Override must specify fixed_ticker, fixed_issuer, or forced_null=True")
    return cleaned
