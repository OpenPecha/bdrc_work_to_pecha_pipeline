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
from bdrc_work_to_pecha_pipeline.metadata import (
    get_buda_data,
    get_metadata,
    get_ocr_import_info,
)
from bdrc_work_to_pecha_pipeline.utils import zip_folder


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
        print("Uploading Pecha to OpenPecha API...")
        response = requests.post(url, data=form_data, files=files)
        print(f"API response code: {response.status_code}")

        if response.ok:
            print("✅ Pecha created successfully!")
            print(response.json())
        else:
            print("❌ Failed to create Pecha")
            print(response.text)
    except Exception as e:
        print(f"❌ Error during API request: {e}")
        if "response" in locals():
            print(response.text)


def run_pipeline(
    work_id: str, batch_number: str, ocr_engine: str, base_data_dir: str = "./data"
):
    """
    Full pipeline: Download OCR data, extract metadata, zip folder, and send to OpenPecha API.
    """
    print(
        f"\n🚀 Running pipeline for work ID: {work_id}, batch: {batch_number}, engine: {ocr_engine}"
    )

    work_path = Path(base_data_dir) / work_id

    # Step 1: Download OCR files
    print("📥 Downloading OCR data...")
    download_ocr_data(work_id, batch_number, ocr_engine)

    # Step 2: Generate metadata
    print("📝 Generating metadata...")
    metadata = generate_metadata(work_path, ocr_engine, batch_number)

    # Step 3: Zip the OCR folder
    print("📦 Creating zip archive...")
    zip_path = zip_folder(work_path)

    # Step 4: Upload to OpenPecha
    print("☁️ Uploading to OpenPecha API...")
    create_pecha(metadata, data_file=zip_path)


if __name__ == "__main__":
    work_id = "W24767"
    batch_number = "batch001"
    ocr_engine = OcrEngine.GOOGLE_VISION
    run_pipeline(work_id=work_id, batch_number=batch_number, ocr_engine=ocr_engine)
