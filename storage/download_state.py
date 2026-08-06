"""
Persistent download state management.

Stores resumable download progress for each dataset.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from datetime import datetime, timezone

from storage.config import METADATA_DIR


STATE_DIR = METADATA_DIR / "download_state"
STATE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class DownloadState:
    dataset: str
    checkpoint: str | None = None
    skip: int = 0
    rows_downloaded: int = 0
    status: str = "new"
    retries: int = 0
    updated_at: str | None = None

    def touch(self):
        self.updated_at = datetime.now(
            timezone.utc
        ).isoformat()


def _state_file(dataset: str) -> Path:
    return STATE_DIR / f"{dataset}.json"


def load_state(dataset: str) -> DownloadState:

    file = _state_file(dataset)

    if not file.exists():
        return DownloadState(dataset=dataset)

    with open(file, "r", encoding="utf8") as f:
        data = json.load(f)

    return DownloadState(**data)


def save_state(state: DownloadState):

    state.touch()

    with open(
        _state_file(state.dataset),
        "w",
        encoding="utf8"
    ) as f:

        json.dump(
            asdict(state),
            f,
            indent=4
        )


def delete_state(dataset: str):

    file = _state_file(dataset)

    if file.exists():
        file.unlink()