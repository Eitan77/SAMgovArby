"""resolver — Drop-in ticker resolver package for SAMgovArby.

Public API (use these):
    from resolver import resolve_contracts, resolve_entities, refresh_reference_data, explain_resolution

Backward-compat shims (for code that imported from ticker_resolver_v4):
    from resolver import TickerResolverV4, resolve_ticker
"""
from resolver.api import (
    resolve_contracts,
    resolve_entities,
    refresh_reference_data,
    explain_resolution,
    resolve_v1,
)
from resolver.models import (
    ResolverConfig,
    load_config,
    get_default_config,
    ReferenceHandles,
    FinalResolution,
    OverrideRecord,
    ResolverStatus,
)

# ── Backward-compatibility shim ───────────────────────────────────────────────
# Allows existing code using ticker_resolver_v4 imports to keep working
# after that file is deleted.

from resolver._compat import TickerResolverV4, resolve_ticker

__all__ = [
    # New public API
    "resolve_contracts",
    "resolve_entities",
    "refresh_reference_data",
    "explain_resolution",
    # Config / models
    "ResolverConfig",
    "load_config",
    "get_default_config",
    "ReferenceHandles",
    "FinalResolution",
    "OverrideRecord",
    "ResolverStatus",
    # V1 pipeline
    "resolve_v1",
    # Backward compat
    "TickerResolverV4",
    "resolve_ticker",
]
