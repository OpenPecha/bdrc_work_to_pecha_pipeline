"""
Module for tracking relationships between pechas created from the same work.
"""
import json
import os
from pathlib import Path
from typing import Dict, Optional

from bdrc_work_to_pecha_pipeline.logger import get_logger

logger = get_logger(__name__)

# Registry file to store work_id -> first_pecha_id mappings
REGISTRY_FILE = Path(os.path.expanduser("~/.openpecha/pecha_registry.json"))


def ensure_registry_exists():
    """Ensure the registry file and directory exist."""
    if not REGISTRY_FILE.parent.exists():
        REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not REGISTRY_FILE.exists():
        with open(REGISTRY_FILE, "w") as f:
            json.dump({}, f, indent=2)


def get_registry() -> Dict[str, str]:
    """
    Get the current registry mapping work_ids to their first pecha_id.

    Returns:
        Dict mapping work_ids to pecha_ids
    """
    ensure_registry_exists()

    try:
        with open(REGISTRY_FILE) as f:
            return json.load(f)
    except json.JSONDecodeError:
        # If the file is empty or corrupted, return an empty dict
        return {}


def register_pecha(work_id: str, pecha_id: str) -> None:
    """
    Register a pecha as the first version for a work_id if not already registered.

    Args:
        work_id: The BDRC work ID
        pecha_id: The OpenPecha ID
    """
    registry = get_registry()

    # Only register if this work_id doesn't already have a first pecha
    if work_id not in registry:
        registry[work_id] = pecha_id

        # Save the updated registry
        with open(REGISTRY_FILE, "w") as f:
            json.dump(registry, f, indent=2)

        logger.info(
            f"Registered pecha {pecha_id} as the first version for work {work_id}"
        )


def get_first_pecha_for_work(work_id: str) -> Optional[str]:
    """
    Get the first pecha ID created for a work_id.

    Args:
        work_id: The BDRC work ID

    Returns:
        The pecha ID of the first version, or None if not found
    """
    registry = get_registry()
    return registry.get(work_id)
