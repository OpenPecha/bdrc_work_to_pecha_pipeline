"""
Module for tracking relationships between pechas created from the same work.
"""
import json
from pathlib import Path
from typing import Dict, Optional

from botocore.exceptions import ClientError

from bdrc_work_to_pecha_pipeline.config import OCR_OUTPUT_BUCKET, s3_client
from bdrc_work_to_pecha_pipeline.logger import get_logger

logger = get_logger(__name__)

# Registry file to store work_id -> first_pecha_id mappings
REGISTRY_FILE = Path("pecha_registry.json")

# S3 configuration for registry sync
# Use the OCR_OUTPUT_BUCKET and store under a dedicated folder/prefix
S3_BUCKET = OCR_OUTPUT_BUCKET
S3_KEY = "work_to_pecha/pecha_registry.json"


def ensure_registry_exists() -> None:
    """Ensure the registry file and directory exist."""
    if not REGISTRY_FILE.parent.exists():
        REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not REGISTRY_FILE.exists():
        with open(REGISTRY_FILE, "w") as f:
            json.dump({}, f, indent=2)


def download_registry_from_s3() -> None:
    """Download the registry file from S3 if it exists."""
    try:
        ensure_registry_exists()  # Ensure directory exists
        s3_client.download_file(S3_BUCKET, S3_KEY, str(REGISTRY_FILE))
        logger.info(f"Downloaded registry from s3://{S3_BUCKET}/{S3_KEY}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":  # noqa: E231
            logger.info("No registry file found in S3; starting fresh.")
        else:
            logger.error(f"Failed to download registry from S3: {e}")


def upload_registry_to_s3() -> None:
    """Upload the registry file to S3."""
    try:
        s3_client.upload_file(str(REGISTRY_FILE), S3_BUCKET, S3_KEY)
        logger.info(f"Uploaded registry to s3://{S3_BUCKET}/{S3_KEY}")
    except ClientError as e:
        logger.error(f"Failed to upload registry to S3: {e}")  # noqa: E231


def get_registry() -> Dict[str, str]:
    """
    Get the current registry mapping work_ids to their first pecha_id.

    Returns:
        Dict mapping work_ids to pecha_ids
    """
    download_registry_from_s3()  # Always fetch latest before reading
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
        upload_registry_to_s3()  # Sync to S3 after update


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
