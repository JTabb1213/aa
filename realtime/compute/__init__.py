"""
Compute module — handles all price aggregation and comparison logic.

This separates computation from I/O (Redis writes), making the
codebase cleaner and easier to test.
"""

from compute.aggregator import PriceAggregator

__all__ = ["PriceAggregator"]
