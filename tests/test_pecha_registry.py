import json
from unittest.mock import patch

import pytest

from bdrc_work_to_pecha_pipeline import pecha_registry


@pytest.fixture
def dummy_registry(tmp_path, monkeypatch):
    # Use a temp file for the registry
    reg_file = tmp_path / "pecha_registry.json"
    monkeypatch.setattr(pecha_registry, "REGISTRY_FILE", reg_file)
    return reg_file


@patch("bdrc_work_to_pecha_pipeline.pecha_registry.download_registry_from_s3")
@patch("bdrc_work_to_pecha_pipeline.pecha_registry.upload_registry_to_s3")
def test_register_and_get_first_pecha(mock_upload, mock_download, dummy_registry):
    # Simulate empty registry
    with open(dummy_registry, "w") as f:
        json.dump({}, f)
    work_id = "W123"
    pecha_id = "P456"
    # Register and check retrieval
    pecha_registry.register_pecha(work_id, pecha_id)
    assert pecha_registry.get_first_pecha_for_work(work_id) == pecha_id
    # Registering again with a different pecha_id should NOT overwrite
    pecha_registry.register_pecha(work_id, "P789")
    assert pecha_registry.get_first_pecha_for_work(work_id) == pecha_id


@patch("bdrc_work_to_pecha_pipeline.pecha_registry.download_registry_from_s3")
def test_get_first_pecha_for_nonexistent_work(mock_download, dummy_registry):
    # Simulate a registry with only one work
    with open(dummy_registry, "w") as f:
        json.dump({"W111": "P222"}, f)
    assert pecha_registry.get_first_pecha_for_work("W999") is None
