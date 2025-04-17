import json
from pathlib import Path
from typing import Any, Dict

from openpecha.bdrc_utils import extract_metadata_for_work, format_metadata_for_op_api
from openpecha.buda.api import get_buda_scan_info


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
