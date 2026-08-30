#!/usr/bin/env python3
"""Create every topic in the 'Complete topic list' (idempotent)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline.infra import kafka_utils

if __name__ == "__main__":
    kafka_utils.ensure_topics()
    print(f"topics ready: {kafka_utils.config.TOPICS}")
