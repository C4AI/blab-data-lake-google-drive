#!/usr/bin/env python3

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from blabgddatalake import __main__  # noqa: E402, F401
