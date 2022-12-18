# Parquet-Inspector

A command line tool for inspecting parquet files with PyArrow.

## Installation

```bash
pip install parquet-inspector
```

## Usage

```txt
parquet-inspector: cli tool for inspecting parquet files.

positional arguments:
  {metadata,schema,head,tail,count,validate,to-jsonl,to-parquet}
    metadata            print file metadata
    schema              print data schema
    head                print first n rows (default is 10)
    tail                print last n rows (default is 10)
    count               print number of rows
    validate            validate file
    to-jsonl            convert parquet file to jsonl
    to-parquet          convert jsonl file to parquet

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --threads, -t         use threads for reading
  --mmap, -m            use memory mapping for reading

```

## Examples

```bash
# Print the metadata of a parquet file
$ pqi metadata my_file.parquet
created_by: parquet-cpp-arrow version 6.0.1
num_columns: 3
num_rows: 2
num_row_groups: 1
format_version: 1.0
serialized_size: 818
```

```bash
# Print the schema of a parquet file
$ pqi schema my_file.parquet
a: list<item: int64>
  child 0, item: int64
b: struct<c: bool, d: timestamp[ms]>
  child 0, c: bool
  child 1, d: timestamp[ms]
```

```bash
# Print the first 5 rows of a parquet file (default is 10)
$ pqi head -n 5 my_file.parquet
{"a": 1, "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": 2, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 3, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 4, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 5, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
```

```bash
# Print the last 5 rows of a parquet file
$ pqi tail -n 5 my_file.parquet
{"a": 3, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 4, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 5 "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 6 "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 7 "b": {"c": true, "d": "2019-04-01 00:00:00"}}
```

```bash
# Print the first 5 rows of a parquet file, only reading the column a
$ pqi head -n 5 -c a my_file.parquet
{'a': 1}
{'a': 2}
{'a': 3}
{'a': 4}
{'a': 5}
```

```bash
# Print the first 3 rows that satisfy the condition a > 3
# (filters are defined in disjunctive normal form)
$ pqi head -n 3 -f "[('a', '>', 3)]" my_file.parquet
{"a": 4, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 5 "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 6 "b": {"c": false, "d": "2019-04-01 00:00:00"}}
```

```bash
# Print the number of rows in a parquet file
$ pqi count my_file.parquet
7
```

```bash
# Validate a parquet file
$ pqi validate my_file.parquet
OK
```

```bash
# Convert a parquet file to jsonl
$ pqi to-jsonl my_file.parquet
$ cat my_file.jsonl
{"a": 1, "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": 2, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 3, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 4, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 5, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 6, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 7, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
```

```bash
# Convert a jsonl file to parquet
$ pqi to-parquet my_file.jsonl
$ pqi head my_file.parquet
{"a": 1, "b": {"c": true, "d": "1991-02-03 00:00:00"}}
{"a": 2, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 3, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 4, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 5, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
{"a": 6, "b": {"c": false, "d": "2019-04-01 00:00:00"}}
{"a": 7, "b": {"c": true, "d": "2019-04-01 00:00:00"}}
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
