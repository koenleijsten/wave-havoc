import os
import re
from pyspark.sql import functions as F
from dotenv import load_dotenv
from pyspark.sql.window import Window
from spark import Spark
from get_data import get_data, extract_data, remove_existing_data
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


def main():
    # Set main directory
    main_dir = f"{os.path.dirname(os.path.realpath(__file__))}/../.."

    # Load ENV variables
    load_dotenv()
    NUM_WORKERS = int(os.getenv("NUM_WORKERS"))
    LOCATION = os.getenv("LOCATION")
    THRESHOLD_TEMP = int(os.getenv("THRESHOLD_TEMP"))
    TROPICAL_DAY_TEMP = int(os.getenv("TROPICAL_DAY_TEMP"))
    DURATION_IN_DAYS_THRESHOLD = int(os.getenv("DURATION_IN_DAYS_THRESHOLD"))
    NUMBER_OF_TROPICAL_DAYS_THRESHOLD = int(
        os.getenv("NUMBER_OF_TROPICAL_DAYS_THRESHOLD")
    )

    # Remove existing data if it exists
    remove_existing_data(output_dir=main_dir)

    # Download and extract data
    url = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"
    get_data(url=url, output_dir=main_dir)
    extract_data(output_dir=main_dir, filter="data")

    # Get raw data file locations
    data_directory = f"{main_dir}/data/"
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
    df_init = spark.createDataFrame(data, schema=schema)

    # Select relevant Columns and filter on "De Bilt"
    df_filtered = df_init.select("DTG", "LOCATION", "TX_DRYB_10").filter(
        df_init["LOCATION"] == LOCATION
    )

    # Calculate max temperature per date
    df_grouped = df_filtered.groupBy(
        F.date_format(F.col("DTG"), "yyyy-MM-dd").alias("date"),
        F.col("LOCATION").alias("location"),
    ).agg(F.max("TX_DRYB_10").alias("max_temperature"))

    # Repartition
    df_grouped = df_grouped.repartition(numPartitions=NUM_WORKERS)

    # Order by date ASC
    df_ordered = df_grouped.orderBy(F.asc("date"))

    # Add boolean column for tropical day
    df_tropical = df_ordered.withColumn(
        "tropical_day",
        F.when(F.col("max_temperature") > TROPICAL_DAY_TEMP, 1).otherwise(0),
    )

    # Add boolean column for day above threshold
    df_threshold = df_tropical.withColumn(
        "above_threshold",
        F.when(F.col("max_temperature") > THRESHOLD_TEMP, 1).otherwise(0),
    )

    # Only select days above threshold
    df_threshold_filtered = df_threshold.filter(F.col("above_threshold") != 0)

    # Define a window specification for ordering by date
    window = Window.partitionBy("location").orderBy("date")

    # Use lag function to check the previous date
    df_with_lag = df_threshold_filtered.withColumn(
        "prev_date", F.lag("date", 1).over(window)
    )

    # Add column to calculate the date difference with previous row
    df_date_diff = df_with_lag.withColumn(
        "date_diff", F.datediff(df_with_lag["date"], df_with_lag["prev_date"])
    )

    # Add column to flag each consecutive days run start
    df_flag = df_date_diff.withColumn(
        "flag_run",
        F.when((F.col("date_diff") > 1) | (F.col("date_diff").isNull()), 1).otherwise(
            0
        ),
    )

    # Add column to assign group id's to consecutive day runs
    df_group_id = df_flag.withColumn("group_id", F.sum("flag_run").over(window))

    # Aggregate to final DataFrame
    df_result = (
        df_group_id.groupBy("group_id")
        .agg(
            F.min("date").alias("from_date"),
            F.max("date").alias("to_date"),
            F.sum("above_threshold").alias("duration_in_days"),
            F.sum("tropical_day").alias("number_of_tropical_days"),
            F.max("max_temperature").alias("max_temperature"),
        )
        .select(
            "from_date",
            "to_date",
            "duration_in_days",
            "number_of_tropical_days",
            "max_temperature",
        )
    )

    # Only select heatwaves
    df_result = df_result.filter(
        (F.col("duration_in_days") >= DURATION_IN_DAYS_THRESHOLD)
        & (F.col("number_of_tropical_days") >= NUMBER_OF_TROPICAL_DAYS_THRESHOLD)
    )

    # Convert output to JSON
    output = df_result.toJSON().collect()

    # Stop Spark Session
    spark.stop()

    return output


if __name__ == "__main__":
    main()
