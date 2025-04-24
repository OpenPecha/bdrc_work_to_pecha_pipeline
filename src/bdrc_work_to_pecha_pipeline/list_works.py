"""
Script to list all work IDs in the S3 bucket.
# To list all work IDs
python -m bdrc_work_to_pecha_pipeline.list_works

# To show detailed information about each work
python -m bdrc_work_to_pecha_pipeline.list_works --details

# To save the results to a file
python -m bdrc_work_to_pecha_pipeline.list_works --output work_ids.txt

# To save detailed information to a JSON file
python -m bdrc_work_to_pecha_pipeline.list_works --details --output work_details.json
"""
import argparse
from typing import Dict, List

from bdrc_work_to_pecha_pipeline.config import OCR_OUTPUT_BUCKET, s3_client
from bdrc_work_to_pecha_pipeline.download import get_s3_prefix
from bdrc_work_to_pecha_pipeline.logger import get_logger

logger = get_logger(__name__)


def list_all_hash_directories() -> List[str]:
    """
    List all hash directories under the 'Works/' prefix.

    Returns:
        List of hash directory prefixes (e.g., 'Works/a1/')
    """
    hash_dirs = set()

    try:
        # List objects with delimiter to get "directories"
        response = s3_client.list_objects_v2(
            Bucket=OCR_OUTPUT_BUCKET, Prefix="Works/", Delimiter="/"
        )

        # Extract the common prefixes (directories)
        if "CommonPrefixes" in response:
            for prefix in response["CommonPrefixes"]:
                hash_dirs.add(prefix["Prefix"])

        return sorted(list(hash_dirs))
    except Exception as e:
        logger.error(f"Error listing hash directories: {e}")
        return []


def list_work_ids_in_hash_dir(hash_dir: str) -> List[str]:
    """
    List all work IDs within a specific hash directory.

    Args:
        hash_dir: The hash directory prefix (e.g., 'Works/a1/')

    Returns:
        List of work IDs
    """
    work_ids = set()
    continuation_token = None

    try:
        while True:
            # Prepare the list_objects_v2 parameters
            params = {"Bucket": OCR_OUTPUT_BUCKET, "Prefix": hash_dir, "Delimiter": "/"}

            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            # Extract work IDs from common prefixes
            if "CommonPrefixes" in response:
                for prefix in response["CommonPrefixes"]:
                    # Extract work ID from prefix (e.g., 'Works/a1/W1234/' -> 'W1234')
                    prefix_parts = prefix["Prefix"].strip("/").split("/")
                    if len(prefix_parts) >= 3:
                        work_id = prefix_parts[2]
                        work_ids.add(work_id)

            # Check if there are more results
            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        return sorted(list(work_ids))
    except Exception as e:
        logger.error(f"Error listing work IDs in {hash_dir}: {e}")
        return []


def list_all_work_ids() -> List[str]:
    """
    List all work IDs across all hash directories.

    Returns:
        List of all work IDs
    """
    all_work_ids = []

    # Get all hash directories
    hash_dirs = list_all_hash_directories()
    logger.info(f"Found {len(hash_dirs)} hash directories")

    # For each hash directory, get all work IDs
    for hash_dir in hash_dirs:
        logger.info(f"Processing hash directory: {hash_dir}")
        work_ids = list_work_ids_in_hash_dir(hash_dir)
        all_work_ids.extend(work_ids)
        logger.info(f"Found {len(work_ids)} work IDs in {hash_dir}")

    logger.info(f"Total work IDs found: {len(all_work_ids)}")
    return all_work_ids


def get_work_details() -> Dict[str, Dict]:
    """
    Get detailed information about each work ID, including available OCR engines and batches.

    Returns:
        Dictionary mapping work IDs to their details
    """
    work_details = {}
    all_work_ids = list_all_work_ids()

    for work_id in all_work_ids:
        try:
            # Get the S3 prefix for this work ID
            work_prefix = get_s3_prefix(work_id)

            # List all OCR engines for this work
            ocr_engines = set()
            response = s3_client.list_objects_v2(
                Bucket=OCR_OUTPUT_BUCKET, Prefix=work_prefix, Delimiter="/"
            )

            if "CommonPrefixes" in response:
                for prefix in response["CommonPrefixes"]:
                    # Extract OCR engine from prefix
                    prefix_parts = prefix["Prefix"].strip("/").split("/")
                    if len(prefix_parts) >= 4:
                        ocr_engine = prefix_parts[3]
                        ocr_engines.add(ocr_engine)

            # For each OCR engine, list all batches
            engine_batches = {}
            for engine in ocr_engines:
                engine_prefix = f"{work_prefix}{engine}/"
                batches = set()

                batch_response = s3_client.list_objects_v2(
                    Bucket=OCR_OUTPUT_BUCKET, Prefix=engine_prefix, Delimiter="/"
                )

                if "CommonPrefixes" in batch_response:
                    for prefix in batch_response["CommonPrefixes"]:
                        # Extract batch from prefix
                        prefix_parts = prefix["Prefix"].strip("/").split("/")
                        if len(prefix_parts) >= 5:
                            batch = prefix_parts[4]
                            batches.add(batch)

                if batches:
                    engine_batches[engine] = sorted(list(batches))

            # Add work details to the dictionary
            if engine_batches:
                work_details[work_id] = {
                    "ocr_engines": sorted(list(ocr_engines)),
                    "engine_batches": engine_batches,
                }
        except Exception as e:
            logger.error(f"Error getting details for work {work_id}: {e}")

    return work_details


def main():
    """
    Main function to run the script.
    """
    parser = argparse.ArgumentParser(description="List all work IDs in the S3 bucket")
    parser.add_argument(
        "--details",
        action="store_true",
        help="Show detailed information about each work",
    )
    parser.add_argument(
        "--output", type=str, help="Output file to save the list of work IDs"
    )
    args = parser.parse_args()

    if args.details:
        # Get detailed information about each work
        work_details = get_work_details()

        # Print the details
        for work_id, details in work_details.items():
            print(f"Work ID: {work_id}")
            print(f"  OCR Engines: {', '.join(details['ocr_engines'])}")
            print("  Batches:")
            for engine, batches in details["engine_batches"].items():
                print(f"    {engine}: {', '.join(batches)}")
            print()

        # Save to file if requested
        if args.output:
            import json

            with open(args.output, "w") as f:
                json.dump(work_details, f, indent=2)
            print(f"Saved detailed work information to {args.output}")
    else:
        # Just list all work IDs
        all_work_ids = list_all_work_ids()

        # Print the list
        for work_id in all_work_ids:
            print(work_id)

        # Save to file if requested
        if args.output:
            with open(args.output, "w") as f:
                for work_id in all_work_ids:
                    f.write(f"{work_id}\n")
            print(f"Saved work IDs to {args.output}")


if __name__ == "__main__":
    main()
