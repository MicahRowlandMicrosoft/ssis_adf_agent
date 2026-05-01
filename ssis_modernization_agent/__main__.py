"""Allow ``python -m ssis_modernization_agent ...`` to invoke the headless CLI (M3)."""

from __future__ import annotations

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
