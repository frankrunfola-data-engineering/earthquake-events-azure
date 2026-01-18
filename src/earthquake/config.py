# src/earthquake/config.py
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class PipelineConfig:
    base_url: str
    output_dir: Path
    lookback_days: int
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        base_url = os.getenv("API_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
        output_dir = Path(os.getenv("OUTPUT_DIR", "data"))
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "1"))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        return cls(
            base_url=base_url,
            output_dir=output_dir,
            lookback_days=lookback_days,
            log_level=log_level,
        )

    def get_log_level(self) -> str:
        """
        Single source of truth for log level.
        - Prefer config value if set
        - Fallback to env LOG_LEVEL
        - Final fallback INFO
        Normalizes to uppercase.
        """
        log_level:str=''
        level = (self.log_level or "").strip()
        if level:
            log_level = level.upper()
        else:
            log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        return log_level

    def print_config(self, logger: logging.Logger, *, banner: bool = True) -> None:
        """
        Log the effective configuration in an aligned block.

        Note: This intentionally logs only non-secret fields. If you add secrets later,
        DO NOT print them here.
        """
        rows = [
            ("Base URL", self.base_url),
            ("Output dir", str(self.output_dir)),
            ("Lookback days", self.lookback_days),
            ("Log level", self.get_log_level()),
        ]

        widest_label = max(len(k) for k, _ in rows)          # widest label length (for aligned columns)
        widest_value   = max(len(str(v)) for _, v in rows)     # widest value length (for aligned columns)

        if banner:
            logger.info("")
            logger.info("-----------------------------------------------------")
            logger.info("Pipeline configuration")
            logger.info("-----------------------------------------------------")


        for k, v in rows:
            logger.info(f"  {k:<{widest_label}} : {str(v):<{widest_value}}")

