"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    src/earthquake/pipeline.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
Description:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from logging import Logger
from pathlib import Path
from typing import Any

from .config import PipelineConfig
from .enrich.gold import silver_to_gold_df
from .extract.usgs import get_API_data
from .io.fs import write_csv, write_json
from .io.paths import MedallionPaths, build_paths
from .transform.silver import raw_JSON_to_silver_df


@dataclass(frozen=True)
class PipelineResult:
    run_date: date
    paths: MedallionPaths
    records_bronze: int
    records_silver: int
    records_gold: int


def run_pipeline(*, config: PipelineConfig, logger: Logger) -> PipelineResult:
    run_date = date.today()
    lookback = getattr(config, "lookback_days", 1) or 1
    end_date = run_date
    start_date = end_date - timedelta(days=int(lookback))

    logger.info("")
    logger.info("-----------------------------------------------------")
    logger.info("Pipeline Run")
    logger.info("-----------------------------------------------------")

    output_dir = Path(getattr(config, "output_dir", "data"))
    paths = build_paths(output_dir=output_dir, run_date=run_date, logger=logger)

    ########################################################
    #                    BRONZE
    ########################################################
    bronze = get_API_data(
        base_url=config.base_url,
        start_date=start_date,
        end_date=end_date,
    )
    paths.bronze_dir.mkdir(parents=True, exist_ok=True)
    write_json(bronze, paths.bronze_dir / "usgs_features.json", logger=logger)

    ########################################################
    #                    SILVER
    ########################################################
    silver_df = raw_JSON_to_silver_df(bronze, run_date=run_date)
    paths.silver_dir.mkdir(parents=True, exist_ok=True)
    write_csv(silver_df, paths.silver_dir / "earthquakes_silver.csv", logger=logger)

    ########################################################
    #                    GOLD
    ########################################################
    gold_df = silver_to_gold_df(silver_df)
    paths.gold_dir.mkdir(parents=True, exist_ok=True)
    write_csv(gold_df, paths.gold_dir / "earthquakes_gold.csv", logger=logger)

    pipelinResult = PipelineResult(
        run_date, paths, bronze_count(bronze), len(silver_df), len(gold_df)
    )
    return pipelinResult


def bronze_count(payload: Any) -> int:  # USGS payload typically: {"features": [...]}
    if isinstance(payload, dict) and isinstance(payload.get("features"), list):
        return len(payload["features"])
    if isinstance(payload, list):
        return len(payload)
    return 0
