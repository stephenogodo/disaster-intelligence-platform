"""
Cursor Manager

Provides robust cursor management for incremental streaming.

Each dataset maintains:
    - last_refresh
    - last_key

This prevents duplicate reads and missed records when multiple
records share the same timestamp.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from storage.config import(
    CHECKPOINT_FILE,
)


def _load_file() -> Dict:
    """
    Load checkpoint file.
    """

    if not CHECKPOINT_FILE.exists():
        return {}

    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_file(data: Dict) -> None:
    """
    Persist checkpoint file.
    """

    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def load_cursor(dataset: str) -> Dict:
    """
    Returns

    {
        "last_refresh": "...",
        "last_key": "..."
    }
    """

    data = _load_file()

    return data.get(
        dataset,
        {
            "last_refresh": None,
            "last_key": None,
        },
    )


def save_cursor(
    dataset: str,
    last_refresh,
    last_key,
) -> None:
    """
    Save cursor.
    """

    data = _load_file()

    data[dataset] = {
        "last_refresh": last_refresh,
        "last_key": str(last_key) if last_key is not None else None,
    }

    _save_file(data)


def reset_cursor(dataset: str) -> None:
    """
    Testing helper.
    """

    data = _load_file()

    if dataset in data:
        del data[dataset]

    _save_file(data)


def advance_cursor(
    cursor: Dict,
    record: Dict,
    checkpoint_field: str,
    key_field: str,
) -> Dict:
    """
    Advance cursor using the latest record.
    """

    return {
        "last_refresh": record.get(checkpoint_field),
        "last_key": record.get(key_field),
    }