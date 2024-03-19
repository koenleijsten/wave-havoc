import os
import re
from datetime import datetime
from typing import List
from pyspark.sql.types import (
    StructType,
    DoubleType,
    TimestampType,
)


def get_data_file_paths(directory_path: str) -> List[str]:
    """List all files in a directory and return their paths.

    Args:
        directory_path (str): The path to the directory containing data files.

    Returns:
        List[str]: A list of file paths.

    Raises:
        OSError: If there's an error while listing files in the directory.
    """
    try:
        return [
            os.path.join(directory_path, file_name)
            for file_name in os.listdir(directory_path)
            if not file_name.startswith(".DS_Store")
        ]
    except OSError as e:
        raise OSError(f"Error occurred while listing files in {directory_path}: {e}")


def get_column_start_positions(header_line: str) -> List[int]:
    """Get the starting positions of each column in the header line.

    This function identifies the starting positions of each column in the header line
    by searching for consecutive whitespace characters.

    Args:
        header_line (str): The header line containing column names.

    Returns:
        List[int]: A list of starting positions for each column.

    Raises:
        ValueError: If the headerline is not properly parsed
    """

    col_starts = [0]
    col_starts.extend(
        match.end() for match in re.finditer(r"\s{2,}", header_line.lstrip("#"))
    )
    if len(col_starts) <= 1:
        raise ValueError("Incorrect header line, no columns found.")
    return col_starts


def parse_line(line: str, col_starts: List[int]) -> List[str]:
    """Parse a line of data based on column starts.

    Args:
        line (str): The line of data to parse.
        col_starts (List[int]): A list of column start positions.

    Returns:
        List[str]: A list of parsed values.

    Raises:
        RuntimeError: If there's an issue while parsing the line.
    """
    try:
        return [
            line[start:end].strip() if end else line[start:].strip()
            for start, end in zip(col_starts, col_starts[1:])
        ]
    except Exception as e:
        raise RuntimeError(f"Error occurred while parsing line: {e}")


def convert_line_values(values: List[str], schema: StructType) -> List:
    """Convert parsed values to correct data types based on schema.

    Args:
        values (List[str]): A list of parsed values.
        schema (StructType): The schema defining the data types of each column.

    Returns:
        List: A list of converted values.

    Raises:
        ValueError: If a conversion fails.
    """
    converted_values = []
    for i, value in enumerate(values):
        field_type = schema.fields[i].dataType
        try:
            if field_type == TimestampType():
                converted_values.append(
                    datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    if value != ""
                    else None
                )
            elif field_type == DoubleType():
                converted_values.append(float(value) if value != "" else None)
            else:
                converted_values.append(value)
        except ValueError as e:
            raise ValueError(
                f"Error converting value '{value}' to type {field_type}: {e}"
            )
    return converted_values
