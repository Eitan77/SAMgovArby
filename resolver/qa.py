"""resolver/qa.py — Diagnostics, evaluation, sampling, explain."""
from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from resolver.models import ExplanationRecord, ResolverStatus

log = logging.getLogger(__name__)

# ── Diagnostics ───────────────────────────────────────────────────────────────

def build_resolution_diagnostics(resolution_df: pd.DataFrame, config) -> dict:
    """Compute summary statistics over a resolved output DataFrame."""
    total = len(resolution_df)
    if total == 0:
        return {"total": 0}
    resolved_mask = resolution_df.get("resolver_status", pd.Series()) == ResolverStatus.RESOLVED.value
    resolved      = int(resolved_mask.sum()) if "resolver_status" in resolution_df.columns else 0
    return {
        "total":           total,
        "resolved":        resolved,
        "resolution_rate": round(resolved / total, 4) if total else 0.0,
        "null_count":      total - resolved,
        "null_rate":       round((total - resolved) / total, 4) if total else 0.0,
        "null_breakdown":  dict(summarize_null_reasons(resolution_df).value_counts()) if "resolver_null_reason" in resolution_df.columns else {},
        "confidence_distribution": dict(resolution_df["resolver_confidence"].value_counts()) if "resolver_confidence" in resolution_df.columns else {},
    }

def summarize_null_reasons(resolution_df: pd.DataFrame) -> pd.Series:
    if "resolver_null_reason" not in resolution_df.columns:
        return pd.Series(dtype=str)
    return resolution_df["resolver_null_reason"].dropna().value_counts()

def summarize_score_distribution(resolution_df: pd.DataFrame) -> pd.DataFrame:
    if "resolver_top_candidate_score" not in resolution_df.columns:
        return pd.DataFrame()
    scores = resolution_df["resolver_top_candidate_score"].dropna()
    bins   = [0, 20, 40, 60, 80, 100]
    labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    return pd.cut(scores, bins=bins, labels=labels, right=True).value_counts().sort_index().reset_index()

def compute_resolution_rate(resolution_df: pd.DataFrame) -> dict:
    total    = len(resolution_df)
    resolved = int((resolution_df.get("resolver_status", pd.Series()) == ResolverStatus.RESOLVED.value).sum())
    return {
        "total":    total,
        "resolved": resolved,
        "rate":     round(resolved / total, 4) if total else 0.0,
    }

def compute_high_value_unresolved(resolution_df: pd.DataFrame) -> pd.DataFrame:
    """Return unresolved rows sorted by contract value descending."""
    null_mask = resolution_df.get("resolver_status", pd.Series()) != ResolverStatus.RESOLVED.value
    unresolved = resolution_df[null_mask].copy()
    if "dollars_obligated" in unresolved.columns:
        unresolved["dollars_obligated"] = pd.to_numeric(unresolved["dollars_obligated"], errors="coerce")
        unresolved = unresolved.sort_values("dollars_obligated", ascending=False)
    return unresolved.head(100)

# ── Evaluation ────────────────────────────────────────────────────────────────

def evaluate_against_labeled_set(
    predictions: pd.DataFrame,
    truth_table:  pd.DataFrame,
    config,
    id_col: str = "contract_row_id",
    label_col: str = "true_ticker",
) -> dict:
    """Compare resolver output to a ground-truth table. Returns precision/recall/etc."""
    merged = predictions.merge(
        truth_table[[id_col, label_col]],
        on=id_col, how="inner",
    )
    if merged.empty:
        return {"error": "No matching rows between predictions and truth table"}
    pred_ticker  = merged.get("resolver_ticker", pd.Series())
    true_ticker  = merged[label_col]
    has_pred     = pred_ticker.notna() & (pred_ticker != "")
    has_true     = true_ticker.notna() & (true_ticker != "")
    tp_mask      = has_pred & has_true & (pred_ticker.str.upper() == true_ticker.str.upper())
    fp_mask      = has_pred & (~tp_mask)
    fn_mask      = (~has_pred) & has_true
    tp = int(tp_mask.sum())
    fp = int(fp_mask.sum())
    fn = int(fn_mask.sum())
    prec   = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    return {
        "precision":    round(prec, 4),
        "recall":       round(recall, 4),
        "true_positives":  tp,
        "false_positives": fp,
        "false_negatives": fn,
        "total_labeled":   len(merged),
    }

def compute_precision(predictions: pd.DataFrame, truth_table: pd.DataFrame) -> float:
    result = evaluate_against_labeled_set(predictions, truth_table, config=None)
    return result.get("precision", 0.0)

def compute_recall(predictions: pd.DataFrame, truth_table: pd.DataFrame) -> float:
    result = evaluate_against_labeled_set(predictions, truth_table, config=None)
    return result.get("recall", 0.0)

def compute_null_rate(predictions: pd.DataFrame) -> float:
    total    = len(predictions)
    resolved = int((predictions.get("resolver_status", pd.Series()) == ResolverStatus.RESOLVED.value).sum())
    return round((total - resolved) / total, 4) if total else 0.0

def compute_false_positive_rate(predictions: pd.DataFrame, truth_table: pd.DataFrame) -> float:
    result = evaluate_against_labeled_set(predictions, truth_table, config=None)
    fp = result.get("false_positives", 0)
    tp = result.get("true_positives", 0)
    return round(fp / (fp + tp), 4) if (fp + tp) > 0 else 0.0

# ── Sampling ──────────────────────────────────────────────────────────────────

def sample_for_manual_review(resolution_df: pd.DataFrame, config, n: int = 50) -> pd.DataFrame:
    """Return a representative sample for manual review."""
    samples = []
    # Low confidence resolved
    if "resolver_confidence" in resolution_df.columns:
        low_conf = resolution_df[
            resolution_df["resolver_confidence"].isin(["low", "low_medium"]) &
            (resolution_df.get("resolver_status", "") == ResolverStatus.RESOLVED.value)
        ]
        samples.append(low_conf.head(n // 3))
    # High value nulls
    samples.append(sample_high_value_nulls(resolution_df, config, n=n // 3))
    # Low gap
    samples.append(sample_low_gap_resolutions(resolution_df, config, n=n // 3))
    if not samples:
        return pd.DataFrame()
    return pd.concat(samples, ignore_index=True).drop_duplicates(subset=["contract_row_id"] if "contract_row_id" in resolution_df.columns else None)

def sample_high_value_nulls(resolution_df: pd.DataFrame, config, n: int = 30) -> pd.DataFrame:
    return compute_high_value_unresolved(resolution_df).head(n)

def sample_low_gap_resolutions(resolution_df: pd.DataFrame, config, n: int = 30) -> pd.DataFrame:
    """Return resolved rows where the score gap is small (most likely to be wrong)."""
    if "resolver_candidate_gap" not in resolution_df.columns:
        return pd.DataFrame()
    resolved = resolution_df[resolution_df.get("resolver_status", pd.Series()) == ResolverStatus.RESOLVED.value].copy()
    resolved["resolver_candidate_gap"] = pd.to_numeric(resolved["resolver_candidate_gap"], errors="coerce")
    return resolved.sort_values("resolver_candidate_gap").head(n)

# ── Explain ───────────────────────────────────────────────────────────────────

def build_explanation_record(resolution_id: str, config, store: dict | None = None) -> ExplanationRecord:
    """Build an ExplanationRecord for a given resolution_id from the audit index or store."""
    if store and resolution_id in store:
        row = store[resolution_id]
    else:
        row = {}
    evidence  = {}
    breakdown = {}
    if row.get("resolver_score_breakdown_json"):
        try:
            breakdown = json.loads(row["resolver_score_breakdown_json"])
        except Exception:
            pass
    if row.get("resolver_evidence_json"):
        try:
            evidence = json.loads(row["resolver_evidence_json"])
        except Exception:
            pass
    return ExplanationRecord(
        resolution_id              = resolution_id,
        awardee_name_norm          = row.get("resolver_awardee_name_norm"),
        parent_name_norm           = row.get("resolver_parent_name_norm"),
        domain_norm                = row.get("resolver_domain_norm"),
        candidates_considered      = evidence.get("candidates", []),
        top_scores                 = [row.get("resolver_top_candidate_score", 0.0)],
        chosen_path                = row.get("resolver_resolution_path"),
        null_reason                = row.get("resolver_null_reason"),
        historical_ticker_evidence = evidence.get("historical", {}),
        security_selection_evidence = evidence.get("security_selection", {}),
    )

def format_explanation_as_dict(rec: ExplanationRecord) -> dict:
    return {
        "resolution_id":               rec.resolution_id,
        "awardee_name_norm":           rec.awardee_name_norm,
        "parent_name_norm":            rec.parent_name_norm,
        "domain_norm":                 rec.domain_norm,
        "candidates_considered":       rec.candidates_considered,
        "top_scores":                  rec.top_scores,
        "chosen_path":                 rec.chosen_path,
        "null_reason":                 rec.null_reason,
        "historical_ticker_evidence":  rec.historical_ticker_evidence,
        "security_selection_evidence": rec.security_selection_evidence,
    }

def format_explanation_as_text(rec: ExplanationRecord) -> str:
    lines = [
        f"Resolution ID:  {rec.resolution_id}",
        f"Awardee:        {rec.awardee_name_norm or 'unknown'}",
        f"Parent:         {rec.parent_name_norm or 'none'}",
        f"Domain:         {rec.domain_norm or 'none'}",
        f"Path chosen:    {rec.chosen_path or 'none'}",
        f"Null reason:    {rec.null_reason or 'n/a'}",
        f"Top score:      {rec.top_scores[0] if rec.top_scores else 0:.1f}",
        f"Candidates:     {len(rec.candidates_considered)}",
    ]
    if rec.historical_ticker_evidence:
        lines.append(f"Historical:     {rec.historical_ticker_evidence}")
    if rec.security_selection_evidence:
        lines.append(f"Security sel.:  {rec.security_selection_evidence}")
    return "\n".join(lines)
