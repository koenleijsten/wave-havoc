import os
import re
from spark import Spark
from get_data import get_data, extract_data
from parse_data import (
    get_data_file_paths,
    get_column_start_positions,
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
    # Set script directory
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Download and extract data
    output_dir = f"{script_dir}/../.."
    # url = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"
    # get_data(url=url, output_dir=output_dir)
    # extract_data(output_dir=output_dir, filter="data")

    # Get raw data file locations
    data_directory = f"{output_dir}/data/"
    data_file_paths = get_data_file_paths(data_directory)

    # Get spark session
    spark = Spark.set_master("local[*]").get_session()

    # Create RDD from text files
    rdd = spark.sparkContext.textFile(",".join(data_file_paths))

    # Filter first header line
    header_pattern = re.compile(r"#\s*DTG.*LOCATION")
    header_line = rdd.filter(lambda line: header_pattern.match(line)).first()

    # Get column starts from header_line
    col_starts = get_column_start_positions(header_line)

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
    data = (
        rdd.filter(lambda line: not line.startswith("#"))
        .map(lambda line: parse_line(line, col_starts))
        .map(lambda values: convert_line_values(values, schema))
    )

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

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
