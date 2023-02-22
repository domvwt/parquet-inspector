import shutil
import tempfile
from pathlib import Path

import pytest
from pyarrow import json as pj  # type: ignore
from pyarrow import parquet as pq  # type: ignore

import parquet_inspector.main as main


def parse_test_args(arg_list: list) -> None:
    parser = main.get_parser()
    args = main._parse_args(parser, parser.parse_args(arg_list))
    return main._process_args(args)


def test_metadata(capsys):
    parse_test_args(["metadata", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
created_by: parquet-cpp-arrow version 6.0.1
num_columns: 3
num_rows: 2
num_row_groups: 1
format_version: 1.0
serialized_size: 818
"""
    )


def test_schema(capsys):
    parse_test_args(["schema", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
a: list<item: int64>
  child 0, item: int64
b: struct<c: bool, d: timestamp[ms]>
  child 0, c: bool
  child 1, d: timestamp[ms]
"""
    )


def test_head(capsys):
    parse_test_args(["head", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_head_n1(capsys):
    parse_test_args(["head", "-n", "1", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
"""
    )


def test_head_columns(capsys):
    parse_test_args(["head", "-c", "a", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2]}
{"a": [3, 4, 5]}
"""
    )


def test_tail(capsys):
    parse_test_args(["tail", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_tail_n1(capsys):
    parse_test_args(["tail", "-n", "1", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_tail_columns(capsys):
    parse_test_args(["tail", "-c", "b", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_count(capsys):
    parse_test_args(["count", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert captured.out == "2\n"


def test_validate(capsys):
    parse_test_args(["validate", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert captured.out == "OK\n"


def test_filters_head(capsys):
    parse_test_args(
        ["head", "-f", "[('a', '>', 5)]", "tests/data/parquet/3.parquet"]
    )
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": 6}
{"a": 7}
{"a": 8}
{"a": 9}
{"a": 10}
"""
    )


def test_filters_tail(capsys):
    parse_test_args(
        ["tail", "-f", "[('a', '<=', 5)]", "tests/data/parquet/3.parquet"]
    )
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": 1}
{"a": 2}
{"a": 3}
{"a": 4}
{"a": 5}
"""
    )


def test_filters_invalid_string(capsys):
    with pytest.raises(SystemExit):
        parse_test_args(
            [
                "head",
                "-f",
                "[(a > 5)]",
                "tests/data/parquet/3.parquet",
            ]
        )
    captured = capsys.readouterr()
    assert captured.out == (
        "Invalid filter string: '[(a > 5)]'\n"
        "(see https://arrow.apache.org/docs/python/generated/"
        "pyarrow.parquet.read_table.html for expected format)\n"
    )


def test_validate_fail(capsys):
    with pytest.raises(SystemExit):
        parse_test_args(["validate", "tests/data/parquet/2.parquet"])
    captured = capsys.readouterr()
    assert captured.out == (
        "Could not open Parquet input source 'tests/data/parquet/2.parquet': "
        "Couldn't deserialize thrift: TProtocolException: Invalid data\n"
    )


def test_to_json_file(capsys):
    with tempfile.NamedTemporaryFile() as f:
        parse_test_args(
            ["to-jsonl", "tests/data/parquet/1.parquet", "-o", f.name]
        )
        f.seek(0)
        assert (
            f.read().decode("utf-8")
            == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
        )


def test_to_json_file_no_output(capsys):
    expected_json = """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f1:
        shutil.copyfile("tests/data/parquet/1.parquet", f1.name)
        parse_test_args(["to-jsonl", f1.name])
        f2 = Path(f1.name).with_suffix(".jsonl")
        actual_json = f2.read_text()
        assert actual_json == expected_json


def test_to_parquet_file(capsys):
    json_str = """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:000"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:000"}}
"""
    with tempfile.NamedTemporaryFile() as f1:
        f1.write(json_str.encode("utf-8"))
        f1.seek(0)
        with tempfile.NamedTemporaryFile() as f2:
            parse_test_args(["to-parquet", f1.name, "-o", f2.name])
            table_pq = pq.read_table(f2.name)
            table_json = pj.read_json(f1.name)
            assert table_pq.equals(table_json)


def test_to_parquet_file_no_output(capsys):
    json_str = """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:000"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:000"}}
"""
    with tempfile.NamedTemporaryFile(suffix=".jsonl") as f1:
        f1.write(json_str.encode("utf-8"))
        f1.seek(0)
        parse_test_args(["to-parquet", f1.name])
        f2 = Path(f1.name).with_suffix(".parquet")
        table_pq = pq.read_table(f2)
        table_json = pj.read_json(f1.name)
        assert table_pq.equals(table_json)


def test_no_subcommand(capsys):
    with pytest.raises(SystemExit):
        parse_test_args([])
    captured = capsys.readouterr()
    print(captured.out)
    assert "usage: pqi [-h] [-v]" in captured.out


def test_threads(capsys):
    parse_test_args(["--threads", "head", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_mmap(capsys):
    parse_test_args(["--mmap", "head", "tests/data/parquet/1.parquet"])
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_read_partitioned(capsys):
    parse_test_args(
        [
            "head",
            "tests/data/parquet/partitioned.parquet",
        ]
    )
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    )


def test_metadata_partitioned(capsys):
    with pytest.raises(SystemExit):
        parse_test_args(
            [
                "metadata",
                "tests/data/parquet/partitioned.parquet",
            ]
        )
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
Error: Use the `metadata` command on a valid partition.
"""
    )


def test_schema_partitioned(capsys):
    with pytest.raises(SystemExit):
        parse_test_args(
            [
                "schema",
                "tests/data/parquet/partitioned.parquet",
            ]
        )
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
Error: Use the `schema` command on a valid partition.
"""
    )


def test_validate_partitioned(capsys):
    parse_test_args(
        [
            "validate",
            "tests/data/parquet/partitioned.parquet",
        ]
    )
    captured = capsys.readouterr()
    assert captured.out == "OK\n"


def test_to_json_file_partitioned_no_output(capsys):
    expected_json = """\
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": [1, 2], "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01 00:00:00"}}
"""
    with tempfile.TemporaryDirectory() as f1:
        new_name = str(Path(f1) / "partitioned.parquet")
        shutil.copytree("tests/data/parquet/partitioned.parquet", new_name)
        parse_test_args(["to-jsonl", new_name])
        f2 = Path(new_name).with_suffix(".jsonl")
        actual_json = f2.read_text()
        assert actual_json == expected_json
