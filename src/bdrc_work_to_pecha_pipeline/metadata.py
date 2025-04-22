import json
from pathlib import Path
from typing import Any, Dict, Optional

from openpecha.buda.api import get_buda_scan_info
from openpecha.utils import read_json


def extract_metadata_for_work(work_path: Path) -> Dict[str, Any]:
    metadata = {}
    ocr_import_info = read_json(work_path / "ocr_import_info.json")
    metadata["ocr_import_info"] = ocr_import_info
    buda_data = read_json(work_path / "buda_data.json")
    metadata["buda_data"] = buda_data

    return metadata


def format_metadata_for_op_api(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formats BDRC metadata into a structure suitable for the OpenPecha API.
    Excludes 'author' and 'title' keys if their corresponding values are None.

    Args:
        metadata: A dictionary containing the raw BDRC metadata.

    Returns:
        A dictionary with the formatted metadata.
    """
    buda_data = metadata.get("buda_data", {}).get("source_metadata", {})
    ocr_info = metadata.get("ocr_import_info", {})

    formatted_data: Dict[str, Any] = {
        "source_type": "bdrc",
        "bdrc": metadata,
        "document_id": ocr_info.get("bdrc_scan_id"),
        "language": (
            buda_data.get("languages", [None])[0]
            if buda_data.get("languages")
            else None
        ),
        "source_url": buda_data.get("id"),
    }

    author: Optional[str] = buda_data.get("author")
    if author:
        formatted_data["author"] = {"bo": author}

    title: Optional[str] = buda_data.get("title")
    if title:
        formatted_data["title"] = {"bo": title}
        formatted_data["long_title"] = {"bo": title}

    return formatted_data


def get_ocr_import_info(work_id_path, ocr_engine, batch_number):
    work_id = work_id_path.name
    ocr_info_path = f"{work_id_path}/info.json"
    if Path(ocr_info_path).exists():
        with open(ocr_info_path) as f:
            ocr_info = json.load(f)
    else:
        ocr_info = {}

    ocr_import_info = {
        "source": "bdrc",
        "software": ocr_engine,
        "batch": batch_number,
        "expected_default_language": "bo",
        "bdrc_scan_id": work_id,
        "ocr_info": ocr_info,
    }
    with open(f"{work_id_path}/ocr_import_info.json", "w") as f:
        json.dump(ocr_import_info, f, indent=4)

    return ocr_import_info


def get_buda_data(work_path: Path) -> Dict[str, Any]:
    work_id = work_path.name
    buda_data = get_buda_scan_info(work_id)
    with open(f"{work_path}/buda_data.json", "w") as f:
        json.dump(buda_data, f, indent=4)
    return buda_data


def get_metadata(work_path: Path) -> Dict[str, Any]:
    metadata = extract_metadata_for_work(work_path)
    metadata = format_metadata_for_op_api(metadata)
    with open(f"{work_path.parent}/{work_path.name}_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    return metadata
