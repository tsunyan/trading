"""Shared local order lifecycle states."""

TERMINAL = frozenset({"FILLED", "CANCELED", "EXPIRED", "ABANDONED"})
