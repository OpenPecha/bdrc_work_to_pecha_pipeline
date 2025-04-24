import unittest
from unittest.mock import patch

import bdrc_work_to_pecha_pipeline.list_works as lw


class TestListWorks(unittest.TestCase):
    @patch("bdrc_work_to_pecha_pipeline.list_works.s3_client")
    def test_list_all_hash_directories(self, mock_s3_client):
        # Setup mock response
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "Works/a1/"},
                {"Prefix": "Works/b2/"},
            ]
        }
        # Call the function
        result = lw.list_all_hash_directories()
        # Assert the result
        self.assertEqual(result, ["Works/a1/", "Works/b2/"])

    @patch("bdrc_work_to_pecha_pipeline.list_works.s3_client")
    def test_list_work_ids_in_hash_dir(self, mock_s3_client):
        # Setup mock response
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "Works/a1/W1234/"},
                {"Prefix": "Works/a1/W5678/"},
            ]
        }
        result = lw.list_work_ids_in_hash_dir("Works/a1/")
        self.assertEqual(result, ["W1234", "W5678"])


if __name__ == "__main__":
    unittest.main()
