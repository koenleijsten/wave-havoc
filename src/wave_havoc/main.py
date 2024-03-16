import os
import re
from spark import Spark
from datetime import datetime
from typing import List
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def get_data_file_paths(directory_path: str) -> List[str]:
    """List all files in a directory and return their paths.

    Args:
        directory_path (str): The path to the directory containing data files.

    Returns:
        List[str]: A list of file paths.
    """
    return [
        os.path.join(directory_path, file_name)
        for file_name in os.listdir(directory_path)
    ]


def parse_line(line: str, col_starts: List[int]) -> List[str]:
    """Parse a line of data based on column starts.

    Args:
        line (str): The line of data to parse.
        col_starts (List[int]): A list of column start positions.

    Returns:
        List[str]: A list of parsed values.
    """
    return [
        line[start:end].strip() if end else line[start:].strip()
        for start, end in zip(col_starts, col_starts[1:])
    ]


def convert_line_values(values: List[str], schema: StructType) -> List:
    """Convert parsed values to correct data types based on schema.

    Args:
        values (List[str]): A list of parsed values.
        schema (StructType): The schema defining the data types of each column.

    Returns:
        List: A list of converted values.
    """
    converted_values = []
    for i, value in enumerate(values):
        field_type = schema.fields[i].dataType
        if field_type == TimestampType():
            converted_values.append(
                datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value != "" else None
            )
        elif field_type == DoubleType():
            converted_values.append(float(value) if value != "" else None)
        else:
            converted_values.append(value)
    return converted_values


if __name__ == "__main__":
    # Get raw data file locations
    directory_path = os.path.join(os.getcwd(), "data", "raw")
    data_file_paths = get_data_file_paths(directory_path)

    # Get spark session
    spark = Spark.set_master("local[*]").get_session()

    # Create RDD from text files
    all_files_rdd = spark.sparkContext.textFile(",".join(data_file_paths))

    # Filter header lines
    header_pattern = re.compile(r"#\s*DTG.*LOCATION")
    header_line = all_files_rdd.filter(lambda line: header_pattern.match(line)).first()

    # Get column starts of header_line
    col_starts = [0] + [
        match.end() for match in re.finditer(r"\s{2,}", header_line.lstrip("#"))
    ]

    # Set Schema of DataFrame
    schema = StructType(
        [
            StructField("DTG", TimestampType(), True),
            StructField("LOCATION", StringType(), True),
            StructField("NAME", StringType(), True),
            StructField("LATITUDE", DoubleType(), True),
            StructField("LONGITUDE", DoubleType(), True),
            StructField("ALTITUDE", DoubleType(), True),
            StructField("U_BOOL_10", StringType(), True),
            StructField("T_DRYB_10", DoubleType(), True),
            StructField("TN_10CM_PAST_6H_10", DoubleType(), True),
            StructField("T_DEWP_10", DoubleType(), True),
            StructField("T_DEWP_SEA_10", DoubleType(), True),
            StructField("T_DRYB_SEA_10", DoubleType(), True),
            StructField("TN_DRYB_10", DoubleType(), True),
            StructField("T_WETB_10", DoubleType(), True),
            StructField("TX_DRYB_10", DoubleType(), True),
            StructField("U_10", DoubleType(), True),
            StructField("U_SEA_10", DoubleType(), True),
        ]
    )

    # Skip header line, filter comment lines, and parse data
    data_rdd = (
        all_files_rdd.filter(lambda line: not line.startswith("#"))
        .map(lambda line: parse_line(line, col_starts))
        .map(lambda values: convert_line_values(values, schema))
    )

    # Create DataFrame
    df = spark.createDataFrame(data_rdd, schema=schema)

    # Filter on De Bilt
    df_filtered = df.filter(df["LOCATION"] == "260_T_a")
    df_filtered.show()

    spark.stop()
