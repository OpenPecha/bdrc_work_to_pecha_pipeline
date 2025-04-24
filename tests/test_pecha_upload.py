from unittest.mock import patch

from bdrc_work_to_pecha_pipeline.pecha_upload import OcrEngine, get_work_batches


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches(mock_get_s3_prefix):
    """
    Test the get_work_batches function with dummy S3 keys.

    This test mocks the get_s3_prefix and get_s3_keys functions to return
    predefined values, allowing us to test the function's logic without
    actually accessing S3.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W1234"
    mock_get_s3_prefix.return_value = f"Works/a1/{work_id}/"

    # We need to handle all three calls to get_s3_keys for the three OCR engines
    with patch(
        "bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys"
    ) as mock_get_s3_keys:
        # Set up a sequence of return values for consecutive calls to get_s3_keys
        mock_get_s3_keys.side_effect = [
            # First call - GOOGLE_VISION_ENGINE returns empty
            [],
            # Second call - GOOGLE_BOOKS returns empty
            [],
            # Third call - GOOGLE_VISION returns our test data
            [
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image1.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image2.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch002/image3.json",
            ],
        ]

        # Call the function under test
        result = get_work_batches(work_id)

        # Verify the results - we should only get the vision batches
        expected_result = [
            (work_id, OcrEngine.GOOGLE_VISION, "batch001"),
            (work_id, OcrEngine.GOOGLE_VISION, "batch002"),
        ]

        # Verify that get_s3_keys was called 3 times (once for each engine)
        assert mock_get_s3_keys.call_count == 3

        # Sort both lists to ensure consistent comparison
        assert sorted(result) == sorted(expected_result)


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches_multiple_engines(mock_get_s3_prefix):
    """
    Test the get_work_batches function with multiple OCR engines.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W1234"
    mock_get_s3_prefix.return_value = f"Works/a1/{work_id}/"

    # We need to patch get_s3_keys multiple times with different return values
    with patch(
        "bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys"
    ) as mock_get_s3_keys:
        # Set up a sequence of return values for consecutive calls to get_s3_keys
        mock_get_s3_keys.side_effect = [
            # First call - GOOGLE_VISION_ENGINE returns empty list
            [],
            # Second call - GOOGLE_BOOKS returns batch003
            [
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_BOOKS}/batch003/page1.html",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_BOOKS}/batch003/page2.html",
            ],
            # Third call - GOOGLE_VISION returns batch001 and batch002
            [
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image1.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image2.json",
                f"Works/a1/{work_id}/{OcrEngine.GOOGLE_VISION}/batch002/image3.json",
            ],
        ]

        # Call the function under test
        result = get_work_batches(work_id)

        # Verify the results
        expected_result = [
            (work_id, OcrEngine.GOOGLE_BOOKS, "batch003"),
            (work_id, OcrEngine.GOOGLE_VISION, "batch001"),
            (work_id, OcrEngine.GOOGLE_VISION, "batch002"),
        ]

        # Verify that get_s3_keys was called 3 times (once for each engine)
        assert mock_get_s3_keys.call_count == 3

        # Sort both lists to ensure consistent comparison
        assert sorted(result) == sorted(expected_result)


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches_no_batches(mock_get_s3_prefix):
    """
    Test the get_work_batches function when no batch directories are found.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W5678"
    mock_get_s3_prefix.return_value = f"Works/a5/{work_id}/"

    # Set up the mock for get_s3_keys to return keys without batch directories
    with patch(
        "bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys"
    ) as mock_get_s3_keys:
        # For all three engines, return values that don't contain batch directories
        mock_get_s3_keys.side_effect = [
            # First call - GOOGLE_VISION_ENGINE
            [
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_VISION_ENGINE}/images/image1.json",
            ],
            # Second call - GOOGLE_BOOKS
            [
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_BOOKS}/metadata/info.json",
            ],
            # Third call - GOOGLE_VISION
            [
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_VISION}/images/image1.json",
                f"Works/a5/{work_id}/{OcrEngine.GOOGLE_VISION}/metadata/info.json",
            ],
        ]

        # Call the function under test
        result = get_work_batches(work_id)

        # Verify that get_s3_keys was called 3 times (once for each engine)
        assert mock_get_s3_keys.call_count == 3

        # Verify the results - should be empty since no batch directories were found
        assert result == []


@patch("bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_prefix")
def test_get_work_batches_exception_handling(mock_get_s3_prefix):
    """
    Test the get_work_batches function's exception handling.
    """
    # Set up the mock for get_s3_prefix
    work_id = "W9012"
    mock_get_s3_prefix.return_value = f"Works/a9/{work_id}/"

    # We need to handle multiple calls with different behaviors
    with patch(
        "bdrc_work_to_pecha_pipeline.pecha_upload.get_s3_keys"
    ) as mock_get_s3_keys:
        # First call raises exception, second and third return values
        mock_get_s3_keys.side_effect = [
            # First call - GOOGLE_VISION_ENGINE
            [
                f"Works/a9/{work_id}/{OcrEngine.GOOGLE_VISION_ENGINE}/batch002/image2.json"
            ],
            # Second call - GOOGLE_BOOKS raises exception
            Exception("Simulated S3 error"),
            # Third call - GOOGLE_VISION
            [f"Works/a9/{work_id}/{OcrEngine.GOOGLE_VISION}/batch001/image1.json"],
        ]

        # Call the function under test
        result = get_work_batches(work_id)

        # Verify that get_s3_keys was called 3 times (once for each engine)
        assert mock_get_s3_keys.call_count == 3

        # Verify the results - should only include the engines that didn't raise exceptions
        expected_result = [
            (work_id, OcrEngine.GOOGLE_VISION_ENGINE, "batch002"),
            (work_id, OcrEngine.GOOGLE_VISION, "batch001"),
        ]

        # Sort both lists to ensure consistent comparison
        assert sorted(result) == sorted(expected_result)


if __name__ == "__main__":
    import pytest

    pytest.main(["-xvs", __file__])
