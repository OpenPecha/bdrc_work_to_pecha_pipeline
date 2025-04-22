from unittest.mock import patch

import pytest

from bdrc_work_to_pecha_pipeline.pecha_upload import OcrEngine, get_work_batches


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys")
@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches(mock_get_s3_prefix, mock_get_s3_keys):
    """
    Test the get_work_batches function with dummy S3 keys.

    This test mocks the get_s3_prefix and get_s3_keys functions to return
    predefined values, allowing us to test the function's logic without
    actually accessing S3.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W1234"
    mock_get_s3_prefix.return_value = f"Works/a1/{work_id}/"

    # Set up the mock for get_s3_keys to return different keys for different engines
    def mock_get_s3_keys_side_effect(prefix):
        if OcrEngine.GOOGLE_VISION in prefix:
            return [
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image1.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image2.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch002/image3.json",
            ]
        elif OcrEngine.GOOGLE_BOOKS in prefix:
            return [
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_BOOKS}/batch003/page1.html",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_BOOKS}/batch003/page2.html",
            ]
        elif OcrEngine.GOOGLE_VISION_ENGINE in prefix:
            # Simulate an empty response for this engine
            return []
        else:
            return []

    mock_get_s3_keys.side_effect = mock_get_s3_keys_side_effect

    # Call the function under test
    result = get_work_batches(work_id)

    # Verify the results
    expected_result = [
        (work_id, OcrEngine.GOOGLE_VISION, "batch001"),
        (work_id, OcrEngine.GOOGLE_VISION, "batch002"),
        (work_id, OcrEngine.GOOGLE_BOOKS, "batch003"),
    ]

    # Sort both lists to ensure consistent comparison
    assert sorted(result) == sorted(expected_result)


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys")
@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches_no_batches(mock_get_s3_prefix, mock_get_s3_keys):
    """
    Test the get_work_batches function when no batch directories are found.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W5678"
    mock_get_s3_prefix.return_value = f"Works/a5/{work_id}/"

    # Set up the mock for get_s3_keys to return keys without batch directories
    def mock_get_s3_keys_side_effect(prefix):
        if OcrEngine.GOOGLE_VISION in prefix:
            return [
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_VISION}/images/image1.json",
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_VISION}/metadata/info.json",
            ]
        else:
            return []

    mock_get_s3_keys.side_effect = mock_get_s3_keys_side_effect

    # Call the function under test
    result = get_work_batches(work_id)

    # Verify the results - should be empty since no batch directories were found
    assert result == []


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys")
@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches_exception_handling(mock_get_s3_prefix, mock_get_s3_keys):
    """
    Test the get_work_batches function's exception handling.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W9012"
    mock_get_s3_prefix.return_value = f"Works/a9/{work_id}/"

    # Set up the mock for get_s3_keys to raise an exception for one engine
    def mock_get_s3_keys_side_effect(prefix):
        if OcrEngine.GOOGLE_VISION in prefix:
            return [
                f"Works/a9/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image1.json",
            ]
        elif OcrEngine.GOOGLE_BOOKS in prefix:
            raise Exception("Simulated S3 error")
        elif OcrEngine.GOOGLE_VISION_ENGINE in prefix:
            return [
                f"Works/a9/{work_id}/{OcrEngine.GOOGLE_VISION_ENGINE}/batch002/image2.json",
            ]
        else:
            return []

    mock_get_s3_keys.side_effect = mock_get_s3_keys_side_effect

    # Call the function under test
    result = get_work_batches(work_id)

    # Verify the results - should only include the engines that didn't raise exceptions
    expected_result = [
        (work_id, OcrEngine.GOOGLE_VISION, "batch001"),
        (work_id, OcrEngine.GOOGLE_VISION_ENGINE, "batch002"),
    ]

    # Sort both lists to ensure consistent comparison
    assert sorted(result) == sorted(expected_result)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
