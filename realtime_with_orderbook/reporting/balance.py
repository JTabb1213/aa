"""
Balance fetching utilities.

Provides a clean interface for fetching balances from exchange REST APIs.
Separated into its own module so it can be extended independently
(e.g. adding balance alerts, historical tracking, etc.).
"""

import logging
from typing import Dict

from execution.manager import ExecutionManager

logger = logging.getLogger(__name__)


async def fetch_all_balances(
    exec_manager: ExecutionManager,
) -> Dict[str, Dict[str, float]]:
    """
    Fetch live balances from all registered exchange clients.

    Returns:
        Dict mapping exchange name → {asset: amount}
        Example: {"kraken": {"usdt": 100.5, "ada": 250.0},
                  "okx":    {"usdt": 99.8,  "ada": 248.0}}
    """
    return await exec_manager.get_all_balances()
