#!/usr/bin/env python
"""Generate the reproducible synthetic raw sources.

Examples:
    python scripts/generate_data.py                  # full volume, default seed
    python scripts/generate_data.py --scale 0.1      # quick local run
    python scripts/generate_data.py --seed 7 --out ./data/raw
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataforge.config import get_settings  # noqa: E402
from dataforge.generators.base import GenConfig  # noqa: E402
from dataforge.generators.run import generate_all  # noqa: E402


def main() -> int:
    s = get_settings()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=s.dataforge_seed)
    p.add_argument(
        "--scale", type=float, default=s.dataforge_scale, help="volume multiplier (1.0 = ~100k orders)"
    )
    p.add_argument("--out", type=Path, default=s.raw_dir)
    p.add_argument("--batches", default="2025-11-30,2025-12-31", help="comma-separated batch cutoff dates")
    p.add_argument(
        "--defect-multiplier", type=float, default=1.0, help="scale injected defect rates (0 = clean data)"
    )
    a = p.parse_args()
    cfg = GenConfig(
        seed=a.seed,
        scale=a.scale,
        batches=[date.fromisoformat(b.strip()) for b in a.batches.split(",") if b.strip()],
        defect_rate_multiplier=a.defect_multiplier,
    )
    summary = generate_all(cfg, a.out)
    print(f"Generated batches {summary['batches']} into {a.out} in {summary['seconds']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
