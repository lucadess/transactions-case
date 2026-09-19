import json

from utils.reader import JSONReader


def _write_json(path, data):
    path.write_text(json.dumps(data))


def test_json_reader_reads_all_json_files_in_directory(spark, tmp_path):
    _write_json(tmp_path / "page_0000000.json", {"items": [{"id": 1}], "limit": 10, "offset": 0, "total": 2})
    _write_json(tmp_path / "page_0000001.json", {"items": [{"id": 2}], "limit": 10, "offset": 1, "total": 2})

    reader = JSONReader(spark, str(tmp_path))
    df = reader.read(spark)

    rows = sorted(df.collect(), key=lambda r: r["offset"])
    assert [r["offset"] for r in rows] == [0, 1]
    assert [r["total"] for r in rows] == [2, 2]


def test_json_reader_ignores_stray_tmp_files(spark, tmp_path):
    _write_json(tmp_path / "page_0000000.json", {"items": [{"id": 1}], "limit": 10, "offset": 0, "total": 1})
    (tmp_path / "page_0000001.json.tmp").write_text("{not, valid, json")

    reader = JSONReader(spark, str(tmp_path))
    df = reader.read(spark)

    assert df.count() == 1
    assert df.collect()[0]["offset"] == 0
