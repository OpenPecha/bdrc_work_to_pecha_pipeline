import json
from pathlib import Path

import requests

from bdrc_work_to_pecha_pipeline.download import (
    download_gb_ocr_files,
    download_gv_ocr_files,
    filter_gb_ocr_keys,
    filter_gv_ocr_keys,
    get_s3_keys,
    get_s3_prefix,
)
from bdrc_work_to_pecha_pipeline.logger import get_logger
from bdrc_work_to_pecha_pipeline.metadata import (
    get_buda_data,
    get_metadata,
    get_ocr_import_info,
)
from bdrc_work_to_pecha_pipeline.utils import zip_folder

logger = get_logger(__name__)


class OcrEngine:
    GOOGLE_VISION_ENGINE = "GoogleVisionEngine"
    GOOGLE_BOOKS = "google_books"
    GOOGLE_VISION = "vision"


def download_ocr_data(work_id: str, batch_number: str, ocr_engine: str):
    """
    Download OCR output from S3 based on the OCR engine.
    """
    s3_prefix = f"{get_s3_prefix(work_id)}{ocr_engine}/{batch_number}/"
    s3_keys = get_s3_keys(s3_prefix)

    if ocr_engine == OcrEngine.GOOGLE_BOOKS:
        keys = filter_gb_ocr_keys(s3_keys)
        downloader = download_gb_ocr_files
    elif ocr_engine in [OcrEngine.GOOGLE_VISION_ENGINE, OcrEngine.GOOGLE_VISION]:
        keys = filter_gv_ocr_keys(s3_keys)
        downloader = download_gv_ocr_files
    else:
        raise ValueError(f"Unsupported OCR engine: {ocr_engine}")

    for key in keys:
        work_id_from_key = key.split("/")[2]
        if key.endswith("info.json"):
            downloader(work_id=work_id_from_key, image_group_id=None, key=key)
        else:
            image_group_id = key.split("/")[6]
            downloader(work_id_from_key, image_group_id, key)


def generate_metadata(work_path: Path, ocr_engine: str, batch_number: str) -> dict:
    """
    Prepare metadata from the downloaded OCR files.
    """
    get_ocr_import_info(work_path, ocr_engine, batch_number)
    get_buda_data(work_path)
    metadata = get_metadata(work_path)

    if "long_title" not in metadata:
        metadata["long_title"] = metadata.get("title", {"bo": "Unknown"})

    return metadata


def create_pecha(metadata: dict, text_file: Path = None, data_file: Path = None):
    """
    Send a multipart/form-data POST request to OpenPecha API to create a Pecha.
    """
    url = "https://api-l25bgmwqoa-uc.a.run.app/pecha"
    form_data = {"metadata": json.dumps(metadata)}
    files = {}

    if text_file:
        files["text"] = (
            text_file.name,
            open(text_file, "rb"),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    elif data_file:
        files["data"] = (data_file.name, open(data_file, "rb"), "application/zip")

    try:
        logger.info("Uploading Pecha to OpenPecha API...")

        response = requests.post(url, data=form_data, files=files)
        logger.info(f"API response code: {response.status_code}")

        if response.ok:
            logger.info("✅ Pecha created successfully!")
            response_json = response.json()
            pecha_id = response_json.get("id")
            title = response_json.get("title")

            # Log the work_id and pecha_id on successful creation
            work_id = metadata.get("document_id")
            logger.info(f"Work ID: {work_id}, Pecha ID: {pecha_id}, Title: {title}")
            logger.info(
                "Pecha details: %s", response_json
            )  # Optionally log the full response
        else:
            logger.error("❌ Failed to create Pecha")
            logger.error("Error response: %s", response.text)
    except Exception as e:
        logger.error(f"❌ Error during API request: {e}")
        if "response" in locals():
            logger.error("Error response: %s", response.text)


def run_pipeline(
    work_id: str, batch_number: str, ocr_engine: str, base_data_dir: str = "./data"
):
    """
    Full pipeline: Download OCR data, extract metadata, zip folder, and send to OpenPecha API.
    """
    logger.info(
        f"\n🚀 Running pipeline for work ID: {work_id}, batch: {batch_number}, engine: {ocr_engine}"
    )

    work_path = Path(base_data_dir) / work_id

    # Step 1: Download OCR files
    logger.info("📥 Downloading OCR data...")
    download_ocr_data(work_id, batch_number, ocr_engine)

    # Step 2: Generate metadata
    logger.info("📝 Generating metadata...")
    metadata = generate_metadata(work_path, ocr_engine, batch_number)

    # Step 3: Zip the OCR folder
    logger.info("📦 Creating zip archive...")
    zip_path = zip_folder(work_path)

    # Step 4: Upload to OpenPecha
    logger.info("☁️ Uploading to OpenPecha API...")
    create_pecha(metadata, data_file=Path(zip_path))


def get_work_batches(work_id: str):
    """
    Discover all OCR engines and batch directories for a given work ID.

    Returns a list of tuples (work_id, ocr_engine, batch_number) for all combinations found.
    """
    result = []
    s3_prefix = get_s3_prefix(work_id)

    # Check all possible OCR engine directories
    ocr_engines = [
        OcrEngine.GOOGLE_VISION_ENGINE,
        OcrEngine.GOOGLE_BOOKS,
        OcrEngine.GOOGLE_VISION,
    ]

    for engine in ocr_engines:
        engine_prefix = f"{s3_prefix}{engine}/"
        try:
            # Get all keys in the engine directory
            engine_keys = get_s3_keys(engine_prefix)

            if not engine_keys:
                continue

            # Extract batch directories (those starting with "batch")
            batch_dirs = set()
            for key in engine_keys:
                # The batch directory should be the 4th element in the path
                # Works/hash/workid/engine/batch_xxx/...
                parts = key.split("/")
                if len(parts) > 4:
                    batch_dir = parts[4]
                    if batch_dir.startswith("batch"):
                        batch_dirs.add(batch_dir)

            # Add all combinations to the result
            for batch in batch_dirs:
                result.append((work_id, engine, batch))

        except Exception as e:
            logger.error(f"Error checking engine {engine}: {e}")

    return result


if __name__ == "__main__":
    work_id = "W24767"
    batch_number = "batch001"
    ocr_engine = OcrEngine.GOOGLE_VISION
    run_pipeline(work_id=work_id, batch_number=batch_number, ocr_engine=ocr_engine)
