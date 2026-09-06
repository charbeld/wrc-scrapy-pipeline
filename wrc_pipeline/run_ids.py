"""Run identifiers.

Every execution gets one id that appears on its log file, its log events, its
MongoDB records, its failure rows and its summary file. That single value is what
lets you answer "what did run X do?" from any of those places.

Format: ``20260902T192705Z-3f9a1c`` - a UTC timestamp for readability plus a
short random suffix so two runs started in the same second cannot collide.
"""

from __future__ import annotations

import time
import uuid


def new_run_id() -> str:
    """Return a fresh run id."""
    return f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:6]}"
