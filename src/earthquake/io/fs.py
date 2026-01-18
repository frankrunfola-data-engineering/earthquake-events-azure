# src/earthquake/io/fs.py
from __future__ import annotations

import json
from pathlib import Path
from logging  import Logger
import pandas as pd


def write_json(obj: object, path: Path, logger: Logger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        logger.info(f"  writing JSON : \"{path}\"")
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_csv(df: pd.DataFrame, path: Path, logger: Logger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"  writing CSV : \"{path}\"")
    df.to_csv(path, index=False)
