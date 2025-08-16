"""Centralized credential resolution for connectors.

Priority:
1) Explicit auth_fields provided in test config
2) Auth broker (e.g., Composio) if configured
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from datamonkey.auth.broker import BaseAuthBroker, ComposioBroker


def _make_broker() -> Optional[BaseAuthBroker]:
    provider = os.getenv("DM_AUTH_PROVIDER")  # e.g., "composio"
    if not provider:
        return None
    if provider == "composio":
        return ComposioBroker()
    raise ValueError(f"Unsupported auth provider: {provider}")


async def resolve_credentials(
    connector_short_name: str, provided_auth_fields: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Resolve credentials for a connector.

    Args:
        connector_short_name: The short name of the connector (e.g., "asana").
        provided_auth_fields: Optional credentials provided in the test config.

    Returns:
        Dict of credentials for the connector.
    """
    if provided_auth_fields:
        return provided_auth_fields

    broker = _make_broker()
    if broker:
        return await broker.get_credentials(connector_short_name)

    raise ValueError(
        f"No credentials provided and no DM_AUTH_PROVIDER configured for {connector_short_name}"
    )


