import os
import re
from spark import Spark
from data_parser import (
    get_data_file_paths,
    parse_line,
    convert_line_values,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

if __name__ == "__main__":
    # Get raw data file locations
    script_dir = os.path.dirname(os.path.realpath(__file__))
    data_file_paths = get_data_file_paths(f"{script_dir}/../../data/raw")

    # Get spark session
    spark = Spark.set_master("local[*]").get_session()

    # Create RDD from text files
    all_files_rdd = spark.sparkContext.textFile(",".join(data_file_paths))

    # Filter first header line
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

    # Filter comment lines, and parse data
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

############################## Stappen ##############################

# 1. Eerst berekenen van gewenste output
# 2. test voor alle bestanden (DONE)
# 3. Output wegschrijven Dataframe, daarna pas berekenen

# Setup correctly
# Write tests
# get request
# Run in Docker
# Include coldwaves

# write logging code
