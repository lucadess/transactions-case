import pytest

from utils.config_manager import get_config


def test_get_config_returns_parsed_yaml(tmp_path):
    config_path = tmp_path / "settings.yml"
    config_path.write_text("catalog: my_catalog\nschemas:\n  bronze: bronze\n")

    config = get_config(str(config_path))

    assert config == {"catalog": "my_catalog", "schemas": {"bronze": "bronze"}}


def test_get_config_missing_file_raises_file_not_found_error(tmp_path):
    missing_path = tmp_path / "does_not_exist.yml"

    with pytest.raises(FileNotFoundError):
        get_config(str(missing_path))
