"""
Checkpoint Manager

Maintains incremental ingestion checkpoints for each FEMA dataset.

Responsibilities
----------------
1. Create checkpoint file if it does not exist
2. Read checkpoint values
3. Update checkpoint values
4. Save checkpoint atomically
"""

from pathlib import Path
import json
import tempfile
import shutil

# ============================================================
# PROJECT PATHS
# ============================================================

from storage.config import (
    METADATA_DIR,
    CHECKPOINT_FILE,
)
METADATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)




# ============================================================
# DEFAULT CHECKPOINT STRUCTURE
# ============================================================
DEFAULT_CHECKPOINT = {

    "declarations": {
        "lastRefresh": None,
        "lastKey": None,
    },

    "public_assistance": {
        "lastRefresh": None,
        "lastKey": None,
    },

    "disaster_summaries": {
        "lastRefresh": None,
        "lastKey": None,
    }

}

# ============================================================
# INTERNAL FUNCTIONS
# ============================================================

def _create_checkpoint_file():
    """
    Create a new checkpoint file with the
    default structure.
    """

    if CHECKPOINT_FILE.exists():
        return

    with open(
        CHECKPOINT_FILE,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            DEFAULT_CHECKPOINT,
            f,
            indent=4,
        )


# ============================================================
# PUBLIC API
# ============================================================

def load_checkpoint():
    """
    Load checkpoint information.

    Automatically creates the checkpoint
    file on first run.
    """

    _create_checkpoint_file()

    with open(
        CHECKPOINT_FILE,
        "r",
        encoding="utf-8",
    ) as f:

        return json.load(f)


def save_checkpoint(data):
    """
    Save checkpoint atomically.
    """

    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        dir=METADATA_DIR,
        encoding="utf-8",
    ) as tmp:

        json.dump(
            data,
            tmp,
            indent=4,
        )

        temp_name = tmp.name

    shutil.move(
        temp_name,
        CHECKPOINT_FILE,
    )


def get_checkpoint(dataset_name):
    """
    Return the lastRefresh checkpoint
    for one dataset.
    """

    checkpoint = load_checkpoint()

    if dataset_name not in checkpoint:

        checkpoint[dataset_name] = {
            "lastRefresh": None
        }

        save_checkpoint(checkpoint)

    return checkpoint[dataset_name]["lastRefresh"]


def update_checkpoint(
    dataset_name,
    timestamp,
):
    """
    Update one dataset checkpoint.
    """

    checkpoint = load_checkpoint()

    checkpoint[dataset_name] = {
        "lastRefresh": timestamp
    }

    save_checkpoint(checkpoint)


def get_cursor(dataset_name):
    """
    Return the complete cursor for a dataset.

    {
        "lastRefresh": "...",
        "lastKey": "..."
    }
    """

    checkpoint = load_checkpoint()

    if dataset_name not in checkpoint:

        checkpoint[dataset_name] = {
            "lastRefresh": None,
            "lastKey": None,
        }

        save_checkpoint(checkpoint)

    # Backward compatibility with old checkpoint files
    checkpoint[dataset_name].setdefault("lastKey", None)

    return checkpoint[dataset_name]


def update_cursor(
    dataset_name,
    last_refresh,
    last_key,
):
    """
    Update the complete cursor.
    """

    checkpoint = load_checkpoint()

    checkpoint[dataset_name] = {
        "lastRefresh": last_refresh,
        "lastKey": str(last_key) if last_key is not None else None,
    }

    save_checkpoint(checkpoint)


def reset_cursor(dataset_name):
    """
    Reset one dataset cursor.
    """

    checkpoint = load_checkpoint()

    checkpoint[dataset_name] = {
        "lastRefresh": None,
        "lastKey": None,
    }

    save_checkpoint(checkpoint)