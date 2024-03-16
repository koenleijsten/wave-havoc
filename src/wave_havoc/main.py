import os
import re
from spark import Spark
from typing import List, Iterator
from datetime import datetime
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def get_data_file_paths(directory_path: str) -> List[str]:
    """List all files in a directory and return them as a list.

    Args:
        directory_path (str): The path to the directory.

    Returns:
        List[str]: A list of filenames in the directory.
    """
    data_file_paths = []
    data_files_names = os.listdir(directory_path)
    for file_name in data_files_names:
        data_file_paths.append(f"{directory_path}/{file_name}")
    return data_file_paths


def filter_lines(
    iter, comment_pattern: re.Pattern, header_pattern: re.Pattern
) -> Iterator[str]:
    """
    Filter function to yield only the first line that matches the header pattern and data lines.

    Args:
        iter (iterable): Iterator over lines.
        header_pattern (re.Pattern): Regular expression pattern for matching header lines.

    Yields:
        str: Lines that match the header pattern.
    """
    header_yielded = False
    for line in iter:
        if header_pattern.match(line) and not header_yielded:
            header_yielded = True
            yield line
        elif not comment_pattern.match(line):
            yield line


def create_rdd(data_file_paths: List[str], spark: SparkSession) -> RDD:
    """
    Create one big RDD by joining the content of multiple data files.

    Args:
        data_file_paths (List[str]): A list of file paths.
        spark (SparkSession): The SparkSession object.

    Returns:
        RDD: One big RDD containing the content from all files.
    """
    # Define a regex pattern to match lines starting with #
    comment_pattern = re.compile(r"^#.*$")
    header_pattern = re.compile(r"#\s*DTG.*LOCATION")

    # Read all files and create one big RDD
    all_files_rdd = spark.sparkContext.wholeTextFiles(
        ",".join(data_file_paths)
    ).values()

    # Apply the filtering function to remove comment lines and yield only the first header line
    filtered_rdd = all_files_rdd.flatMap(lambda text: text.split("\n")).mapPartitions(
        lambda iter: filter_lines(iter, comment_pattern, header_pattern)
    )

    return filtered_rdd


def parse_line(line: str, col_starts: List[int]) -> List[str]:
    """Parse a line of data based on column starts.

    Args:
        line (str): The line of data to parse.
        col_starts (List[int]): A list of column start positions.

    Returns:
        List: A list of parsed values.
    """
    if len(col_starts) > 1:
        values = [
            line[start:end].strip() if end else line[start:].strip()
            for start, end in zip(col_starts, col_starts[1:])
        ]
    else:
        values = [line[start:].strip() for start in col_starts]
    return values


def convert_line_values(values: List[str], schema: StructType) -> List:
    """Convert parsed values to correct data types based on schema.

    Args:
        values (List[str]): A list of parsed values.
        schema (StructType): The schema defining the data types of each column.

    Returns:
        List: A list of converted values.
    """
    casted_values = []
    for i, value in enumerate(values):
        field_type = schema.fields[i].dataType
        if field_type == TimestampType():
            casted_values.append(
                datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value != "" else None
            )
        elif field_type == DoubleType():
            casted_values.append(float(value) if value != "" else None)
        else:
            casted_values.append(value)
    return casted_values


############################## MAIN ##############################

data_file_paths = get_data_file_paths(directory_path=f"{os.getcwd()}/data/raw/")

spark = Spark.set_master("local[*]").get_session()

rdd = create_rdd(data_file_paths=data_file_paths[:3], spark=spark)

# Define the schema
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

header_line = rdd.take(1)[0]

# Calculate the start position of each column
col_starts = [0] + [
    match.end() for match in re.finditer(r"\s{2,}", header_line.lstrip("#"))
]

# Skip the header line and then apply the converter function to each line
data = (
    rdd.zipWithIndex()
    .filter(lambda x: x[1] > 0)
    .map(lambda x: parse_line(x[0], col_starts))
    .map(lambda values: convert_line_values(values, schema))
)

# Convert RDD to DataFrame with specified schema
df = spark.createDataFrame(data, schema=schema)

# Filter on De Bilt
df = df.filter(df["LOCATION"] == "260_T_a")

df.show()

# Stop SparkSession
spark.stop()

############################## Stappen ##############################

# 1. Eerst berekenen van gewenste output
# 2. test voor alle bestanden
# 3. Output wegschrijven Dataframe, daarna pas berekenen

# Setup correctly
# Write tests
# get request
# Run in Docker
# Include coldwaves

# write logging code
