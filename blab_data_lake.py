#!/usr/bin/env python3
"""This script just calls :module:`blabgddatalake`."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from blabgddatalake import __main__  # noqa: E402, F401
