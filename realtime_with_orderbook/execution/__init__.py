"""
Execution layer — handles order placement across exchanges.

Structure:
    types.py        Shared order types
    base.py         Abstract ExchangeClient interface
    manager.py      Routes orders to correct exchange client
    coordinator.py  Two-leg arbitrage execution with test/live mode
    <exchange>.py   Per-exchange trading clients
"""
