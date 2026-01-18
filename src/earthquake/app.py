"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    app.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
Description:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

from dotenv import load_dotenv

from .config import PipelineConfig
from .logging import configure_logging
from .pipeline import run_pipeline


def main(argv: list[str] | None = None) -> int:
    """Everything comes from env/.env via PipelineConfig.from_env()."""

    load_dotenv(override=False)  # Parse .env file and load all nev vars.

    config = PipelineConfig.from_env()
    log_level = config.get_log_level()
    logger = configure_logging(log_level)
    config.print_config(logger)

    result = run_pipeline(config=config, logger=logger)  # main program entry point

    logger.info("")
    logger.info("-----------------------------------------------------")
    logger.info("Pipeline Results")
    logger.info("-----------------------------------------------------")
    logger.info(f"  {'Run date':<15}: {result.run_date.isoformat()}")
    logger.info(f"  {'Bronze records':<15}: {result.records_bronze:<8}")
    logger.info(f"  {'Silver records':<15}: {result.records_silver:<8}")
    logger.info(f"  {'Gold records':<15}: {result.records_gold:<8}")
    logger.info("")

    return 0
