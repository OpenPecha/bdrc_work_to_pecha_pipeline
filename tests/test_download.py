from bdrc_work_to_pecha_pipeline.download import (
    filter_gb_ocr_keys,
    filter_gv_ocr_keys,
    get_hash,
    get_s3_prefix,
)


def test_get_s3_prefix():
    work_id = "W1234"
    expected_s3_prefix = f"Works/a1/{work_id}/"
    s3_prefix = get_s3_prefix(work_id)
    assert s3_prefix == expected_s3_prefix


def test_get_hash():
    work_id = "W1234"
    expected_hash = "a1"
    hash = get_hash(work_id)
    assert hash == expected_hash


def test_filter_gb_ocr_keys():
    s3_keys = [
        "Works/a1/W1234/html.zip",
        "Works/a1/W1234/info.json",
        "Works/a1/W1234/images.zip",
        "Works/a1/W1234/txt.zip",
    ]
    filtered_keys = filter_gb_ocr_keys(s3_keys)
    assert filtered_keys == ["Works/a1/W1234/html.zip", "Works/a1/W1234/info.json"]


def test_filter_gv_ocr_keys():
    s3_keys = [
        "Works/a1/W1234/123.json.gz",
        "Works/a1/W1234/info.json",
        "Works/a1/W1234/images.zip",
        "Works/a1/W1234/txt.zip",
    ]
    filtered_keys = filter_gv_ocr_keys(s3_keys)
    assert filtered_keys == ["Works/a1/W1234/123.json.gz", "Works/a1/W1234/info.json"]


if __name__ == "__main__":
    test_get_s3_prefix()
    test_get_hash()
    test_filter_gb_ocr_keys()
    test_filter_gv_ocr_keys()
