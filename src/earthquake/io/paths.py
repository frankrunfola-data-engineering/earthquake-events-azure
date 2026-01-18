# src/earthquake/io/paths.py
from __future__ import annotations

from logging import Logger
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class MedallionPaths:
    root_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path


def build_paths(*, output_dir: Path, run_date: date, logger:Logger) -> MedallionPaths:
    """
    Standard medallion layout with run partitioning:
      <output_dir>/<layer>/run_date=YYYY-MM-DD/
    """
    run_part = f"run_date={run_date.isoformat()}"
    bronze_dir = output_dir / "bronze" / run_part
    silver_dir = output_dir / "silver" / run_part
    gold_dir = output_dir / "gold" / run_part
    logger.info(f"  {'bronze path':<15}: {bronze_dir}")
    logger.info(f"  {'silver path':<15}: {silver_dir}")
    logger.info(f"  {'golder path':<15}: {gold_dir}")
    logger.info(f"")
    
    return MedallionPaths(
        root_dir=output_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
    )
