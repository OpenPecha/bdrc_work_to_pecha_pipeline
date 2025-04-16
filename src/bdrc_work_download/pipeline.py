from bdrc_work_download.download import (
    download_gb_ocr_files,
    download_gv_ocr_files,
    filter_gb_ocr_keys,
    filter_gv_ocr_keys,
    get_s3_keys,
    get_s3_prefix,
)


class OcrEngine:
    GOOGLE_VISION_ENGINE = "GoogleVisionEngine"
    GOOGLE_BOOKS = "google_books"
    GOOGLE_VISION = "vision"


def download_gv_ocr_output(work_id, batch_number, ocr_engine):
    s3_prefix = get_s3_prefix(work_id)
    s3_prefix += f"{ocr_engine}/{batch_number}/"
    s3_keys = get_s3_keys(s3_prefix)
    keys = filter_gv_ocr_keys(s3_keys)
    for key in keys:
        work_id = key.split("/")[2]
        if key.split("/")[-1] == "info.json":
            download_gv_ocr_files(work_id=work_id, image_group_id=None, key=key)
        else:
            image_group_id = key.split("/")[6]
            download_gv_ocr_files(work_id, image_group_id, key)


def download_gb_ocr_output(work_id, batch_number, ocr_engine):
    s3_prefix = get_s3_prefix(work_id)
    s3_prefix += f"{ocr_engine}/{batch_number}/"
    s3_keys = get_s3_keys(s3_prefix)
    keys = filter_gb_ocr_keys(s3_keys)
    for key in keys:
        work_id = key.split("/")[2]
        if key.split("/")[-1] == "info.json":
            download_gb_ocr_files(work_id=work_id, image_group_id=None, key=key)
        else:
            image_group_id = key.split("/")[6]
            download_gb_ocr_files(work_id, image_group_id, key)


def download_ocr_output_per_engine(work_id: str, batch_number: str, ocr_engine: str):
    """
    Downloads OCR output files by calling a specific function based on the OCR engine.

    Args:
        work_id: The BDRC work ID.
        batch_number: The batch number of the OCR processing.
        ocr_engine: The name of the OCR engine ('google_books', 'GoogleVisionEngine', or 'vision').
    """
    if ocr_engine == OcrEngine.GOOGLE_BOOKS:
        download_gb_ocr_output(work_id, batch_number, ocr_engine)
    elif ocr_engine in [OcrEngine.GOOGLE_VISION_ENGINE, OcrEngine.GOOGLE_VISION]:
        download_gv_ocr_output(work_id, batch_number, ocr_engine)
    else:
        print(f"Unsupported OCR engine: {ocr_engine}")


if __name__ == "__main__":
    work_id = "W1KG10193"
    batch_number = "batch_2022"
    ocr_engine = "GoogleVisionEngine"
    download_ocr_output_per_engine(work_id, batch_number, ocr_engine)
