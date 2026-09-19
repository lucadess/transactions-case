import pytest

from utils.reader import JSONReader, get_reader


def test_get_reader_returns_json_reader_for_directory(tmp_path):
    reader = get_reader(spark=None, source_path=str(tmp_path))

    assert isinstance(reader, JSONReader)
    assert reader.path == str(tmp_path)


def test_get_reader_returns_json_reader_for_json_file_path():
    reader = get_reader(spark=None, source_path="/some/file.json")

    assert isinstance(reader, JSONReader)


def test_get_reader_raises_for_unsupported_extension():
    with pytest.raises(ValueError, match="Unsupported file type"):
        get_reader(spark=None, source_path="/some/file.csv")
