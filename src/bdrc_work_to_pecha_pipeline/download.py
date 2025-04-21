import hashlib
from pathlib import Path
from typing import List, Optional

from bdrc_work_to_pecha_pipeline.config import OCR_OUTPUT_BUCKET, s3_client


def get_hash(work_id):
    md5 = hashlib.md5(str.encode(work_id))
    two = md5.hexdigest()[:2]
    return two


def get_s3_prefix(work_id):
    hash = get_hash(work_id)
    s3_prefix = f"Works/{hash}/{work_id}/"
    return s3_prefix


def get_s3_keys(prefix):
    obj_keys = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=OCR_OUTPUT_BUCKET,
                Prefix=prefix,
                ContinuationToken=continuation_token,
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=OCR_OUTPUT_BUCKET, Prefix=prefix
            )
        if response["Contents"]:
            for obj in response["Contents"]:
                obj_key = obj["Key"]
                obj_keys.append(obj_key)
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break
    return obj_keys


def download_gb_ocr_files(work_id: str, image_group_id: Optional[str], key: str):
    file_name = key.split("/")[-1]

    if file_name == "html.zip":
        download_path = Path(f"./data/{work_id}/output/{image_group_id}/")
        download_path.mkdir(parents=True, exist_ok=True)
        local_file_path = f"{download_path}/html.zip"
    elif file_name == f"TBRC_{image_group_id}.xml" or file_name == "gb-bdrc-map.json":
        download_path = Path(f"./data/{work_id}/info/{image_group_id}/")
        download_path.mkdir(parents=True, exist_ok=True)
        local_file_path = f"{download_path}/{file_name}"
    else:
        download_path = Path(f"./data/{work_id}/")
        download_path.mkdir(parents=True, exist_ok=True)
        local_file_path = f"{download_path}/{file_name}"

    if Path(local_file_path).exists():
        return
    try:
        s3_client.download_file(OCR_OUTPUT_BUCKET, key, local_file_path)
        print(f"Downloaded {file_name} successfully.")
    except Exception as e:
        print(f"Error due to {e}")


def download_gv_ocr_files(work_id: str, image_group_id: Optional[str], key: str):
    file_name = key.split("/")[-1]
    ocr_engine = key.split("/")[3]

    if ocr_engine == "GoogleVisionEngine":
        if image_group_id and "-" in image_group_id:
            image_group_id = "-".join(image_group_id.split("-")[1:])
        else:
            print(
                f"Warning: image_group_id '{image_group_id}' does not contain '-' character."
            )
            image_group_id = None
    else:
        image_group_id = image_group_id

    if file_name.endswith(".json.gz"):
        download_path = Path(f"./data/{work_id}/{image_group_id}/")
        download_path.mkdir(parents=True, exist_ok=True)
        local_file_path = f"{download_path}/{file_name}"
    else:
        download_path = Path(f"./data/{work_id}/")
        download_path.mkdir(parents=True, exist_ok=True)
        local_file_path = f"{download_path}/{file_name}"

    if Path(local_file_path).exists():
        return
    try:
        s3_client.download_file(OCR_OUTPUT_BUCKET, key, local_file_path)
        print(f"Downloaded {file_name} successfully.")
    except Exception as e:
        print(f"Error due to {e}")


def filter_gb_ocr_keys(s3_keys: List[str]) -> List[str]:
    """
    Filters a list of S3 keys to include only those belonging to the
    'google_books/batch_2022' OCR source and excluding 'images.zip' and 'txt.zip'.

    Args:
        s3_keys: A list of S3 key strings.

    Returns:
        A new list containing the filtered S3 key strings.
    """
    filtered_keys = []
    for key in s3_keys:
        parts = key.split("/")
        filename = parts[-1]
        if filename not in ("images.zip", "txt.zip"):
            filtered_keys.append(key)
    return filtered_keys


def filter_gv_ocr_keys(s3_keys: List[str]) -> List[str]:
    """
    Filters a list of S3 keys to include only those belonging to the
    'google_vision/batch_2022' OCR source and excluding 'images.zip' and 'txt.zip'.

    Args:
        s3_keys: A list of S3 key strings.

    Returns:
        A new list containing the filtered S3 key strings.
    """
    filtered_keys = []
    for key in s3_keys:
        parts = key.split("/")
        filename = parts[-1]
        if filename.endswith(".json.gz"):
            filtered_keys.append(key)
        elif filename == "info.json":
            filtered_keys.append(key)
        else:
            continue
    return filtered_keys
