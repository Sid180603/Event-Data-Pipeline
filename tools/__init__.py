"""Operator tooling for the Event Generator slice.

Read-only consumers of a running system: `verify.py` reconciles the ground-truth
ledger against what Kafka stored, `observe.py` scrapes `/metrics` for a live
view. Nothing here is imported by `app` or `driver` -- a tool that the gateway
needs in order to start is not a tool.
"""
