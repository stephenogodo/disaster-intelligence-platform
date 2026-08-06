"""
storage/json_store.py

Generic JSON persistence utilities.

Provides safe, reusable functions for reading, writing
and deleting JSON metadata files.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_json(
    path: Path,
    default: Any = None,
) -> Any:
    """
    Load JSON from disk.

    Returns
    -------
    default
        If the file does not exist or cannot be parsed.
    """

    if not path.exists():
        return default

    try:

        with open(
            path,
            "r",
            encoding="utf-8",
        ) as f:

            return json.load(f)

    except json.JSONDecodeError:

        logger.warning(
            f"Invalid JSON detected: {path}"
        )

        return default

    except OSError as ex:

        logger.warning(
            f"Unable to read {path}: {ex}"
        )

        return default


def save_json(
    path: Path,
    data: Any,
) -> None:
    """
    Save JSON atomically.

    Data is first written to a temporary file before
    replacing the destination file.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_suffix(".tmp")

    try:

        with open(
            temp_path,
            "w",
            encoding="utf-8",
        ) as f:

            json.dump(
                data,
                f,
                indent=4,
            )

        temp_path.replace(path)

    finally:

        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def delete_json(
    path: Path,
) -> None:
    """
    Delete a JSON file if it exists.
    """

    try:

        path.unlink(
            missing_ok=True
        )

    except OSError as ex:

        logger.warning(
            f"Unable to delete {path}: {ex}"
        )


def json_exists(
    path: Path,
) -> bool:
    """
    Return True if the JSON file exists.
    """

    return path.exists()