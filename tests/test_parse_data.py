import os
import re
import unittest
from unittest.mock import patch
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from src.wave_havoc.parse_data import (
    get_data_file_paths,
    get_column_start_positions,
    parse_line,
    convert_line_values,
)


class TestParseData(unittest.TestCase):
    def setUp(self):
        self.schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("surname", StringType(), True),
                StructField("age", DoubleType(), True),
                StructField("DateTime", TimestampType(), True),
            ]
        )

    @patch("os.listdir")
    def test_get_data_file_paths_success(self, mock_listdir):
        mock_listdir.return_value = ["file1.txt", "file2.txt"]

        file_paths = get_data_file_paths("test_data")
        expected_paths = [
            os.path.join("test_data", "file1.txt"),
            os.path.join("test_data", "file2.txt"),
        ]
        self.assertEqual(file_paths, expected_paths)

    @patch("os.listdir")
    def test_get_data_file_paths_failure(self, mock_listdir):
        mock_listdir.side_effect = OSError("Directory not found")

        with self.assertRaises(OSError):
            get_data_file_paths("non_existent_directory")

    def test_get_column_start_positions_success(self):
        header_line = "# Col1   Col2   Col3   Col4"

        col_starts = get_column_start_positions(header_line)
        expected_col_starts = [0, 8, 15, 22]
        self.assertEqual(col_starts, expected_col_starts)

    def test_get_column_start_positions_error_value(self):
        invalid_header_line = "adfadfasdfda"
        with self.assertRaises(ValueError):
            get_column_start_positions(invalid_header_line)

    def test_parse_line_success(self):
        line = "John Doe  30"
        col_starts = [0, 9, 15]

        parsed_values = parse_line(line, col_starts)
        expected_values = ["John Doe", "30"]
        self.assertEqual(parsed_values, expected_values)

    def test_parse_line_error(self):
        line = []
        col_starts = [0, 9, 15]

        with self.assertRaises(RuntimeError):
            parse_line(line, col_starts)

    def test_convert_line_values_success(self):
        values = ["John", "Doe", "30", "2003-07-01 00:10:00"]
        converted_values = convert_line_values(values, self.schema)
        expected_values = ["John", "Doe", 30.0, datetime(2003, 7, 1, 0, 10)]
        self.assertEqual(converted_values, expected_values)

    def test_convert_line_values_error(self):
        values = ["John", "Doe", "Thirty"]
        with self.assertRaises(ValueError):
            convert_line_values(values, self.schema)


if __name__ == "__main__":
    unittest.main()
