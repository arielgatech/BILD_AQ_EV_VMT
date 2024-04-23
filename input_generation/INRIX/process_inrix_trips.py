"""Process INRIX Trips data."""

import argparse
import glob
import itertools
import logging
import shutil
import sys
import time
from argparse import RawTextHelpFormatter
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType

from utils import init_spark, get_spark_session, custom_spark_conf, setup_logging


GROUP_BY_COLUMNS = ("o_GEOID", "d_GEOID", "state", "vehicle_weight_class", "start_hour")
METER_TO_MILE = 0.000621371


def main():
    args = parse_args(sys.argv[1:])
    run_queries(
        args.parquet_file_patterns,
        Path(args.state_fips_codes),
        Path(args.output_dir),
        args.overwrite,
    )


def run_queries(file_patterns, state_fips_codes: Path, output_dir: Path, overwrite: bool):
    """Run the queries with Spark."""
    if output_dir.exists():
        if overwrite:
            shutil.rmtree(output_dir)
        else:
            print(
                f"{output_dir} already exists. Choose a different path or pass --overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)

    output_dir.mkdir()
    start = time.time()
    setup_logging(output_dir / "inrix.log")
    logging.info("Run queries with files %s", " ".join(file_patterns))
    init_spark()
    df, transformed, od_skim, avg_travel_metrics = _run_queries(
        file_patterns, state_fips_codes, output_dir
    )
    duration = time.time() - start
    logging.info("run_queries took %s seconds", duration)
    return df, transformed, od_skim, avg_travel_metrics


def _run_queries(file_patterns, state_fips_codes, output_dir: Path):
    df = read_dataset(file_patterns)
    state_fips_df = read_state_fips_codes(state_fips_codes)
    cleaned_df = write_dataframe(
        clean_data(df, state_fips_df),
        output_dir,
        "cleaned",
    )
    travel_stats_df = calc_travel_stats(cleaned_df)
    avg_travel_metrics_df = write_dataframe(
        calc_avg_travel_metrics_per_trip(travel_stats_df),
        output_dir,
        "travel_metrics",
    )
    return df, cleaned_df, travel_stats_df, avg_travel_metrics_df


def read_dataset(file_patterns) -> DataFrame:
    """Read a list of Parquet files into a DataFrame."""
    spark = get_spark_session()
    files = [str(x) for x in itertools.chain(*(glob.glob(x) for x in file_patterns))]
    if not files:
        raise ValueError(f"No files found with patterns {file_patterns}")
    logging.debug("Read dataset from %s", " ".join(files))
    return spark.read.load(files)


def read_state_fips_codes(filename: Path) -> DataFrame:
    """Read state FIPS codes into a DataFrame."""
    spark = get_spark_session()
    schema = (
        StructType()
        .add("name", StringType(), False)
        .add("state", StringType(), False)
        .add("code", IntegerType(), False)
    )
    return spark.read.csv(str(filename), header=False, schema=schema).drop("name")


def calc_travel_stats(df: DataFrame, group_by_columns=None) -> DataFrame:
    """Return a DataFrame with total travel time, trip distance, and trip count based on the
    specified grouped columns.
    """
    logging.info("Calculate travel stats with group_by_columns=%s", group_by_columns)
    group_by_columns = group_by_columns or GROUP_BY_COLUMNS
    return df.groupBy(*group_by_columns).agg(
        F.sum("travel_time_h").alias("travel_time_h"),
        F.sum("trip_distance_mile").alias("trip_distance_mile"),
        F.count("trip_id").alias("trip_count"),
    )


def calc_avg_travel_metrics_per_trip(df: DataFrame) -> DataFrame:
    """Return a DataFrame with average distance and time per trip."""
    return df.withColumn(
        "distance_per_trip", F.col("trip_distance_mile") / F.col("trip_count")
    ).withColumn("travel_time_per_trip", F.col("travel_time_h") / F.col("trip_count"))


def clean_data(df: DataFrame, state_fips: DataFrame):
    """Perform cleaning and transformations to get the required data format."""
    columns = [
        "trip_id",
        "o_GEOID",
        "d_GEOID",
        "vehicle_weight_class",
        "travel_time_h",
        "trip_distance_mile",
        "state",
        "start_hour",
    ]
    # This time manipulation code might be confusing to readers unfamiliar with Spark.
    # When processing functions like F.hour(col) Spark will convert timestamps to the current
    # computer's time zone. This code will switch the Spark session to UTC temporarily so that
    # it can more easily interpret the timestamps in the time zone of the trip origin location.
    with custom_spark_conf({"spark.sql.session.timeZone": "UTC"}):
        cleaned = (
            df.filter("mode == 1")  # vehicle (not walking mode)
            .filter("movement_type == 1")  # moving trips
            .dropna(subset=["start_cbg", "end_cbg"])  # drop trips with unknown start geography
            .filter("start_dow < 6")  # 1 - 7, Monday - Sunday; exclude weekends
            # .filter("vehicle_weight_class == 1")  # LDVs
            .withColumn("start_state_fips", F.substring("start_cbg", 1, 2).cast(IntegerType()))
            # start_date and end_date are strings in zulu time format.
            .withColumn(
                "start_hour",
                F.hour(F.from_utc_timestamp(F.to_timestamp("start_date"), F.col("start_tz"))),
            )
            .withColumn(
                "travel_time_h",
                (F.unix_timestamp(F.to_timestamp("end_date")) - F.unix_timestamp(F.to_timestamp("start_date"))) / 3600,
            )
            .withColumn("trip_distance_mile", F.col("trip_distance_m") * METER_TO_MILE)
            # Reference: https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html
            # Census Tract STATE+COUNTY+TRACT 2+3+6=11
            .withColumn("o_GEOID", (F.substring("start_cbg", 1, 11)))
            .withColumn("d_GEOID", (F.substring("end_cbg", 1, 11)))
        )

        run_checks(cleaned, state_fips)
        return cleaned.join(state_fips, on=F.col("start_state_fips") == state_fips.code).select(
            *columns
        )


def run_checks(df, state_fips):
    start = time.time()
    check_invalid_travel_times(df)
    check_invalid_states(df, state_fips)
    duration = time.time() - start
    logging.info("checks took %s seconds", duration)


def check_invalid_travel_times(df):
    """Raise an exception if any travel times are less than zero."""
    count = df.filter("travel_time_h < 0").count()
    if count:
        raise ValueError(f"dataframe has trips with travel times less than 0: {count=}")


def check_invalid_states(df, state_fips):
    """Raise an exception if the dataframe has a state abbreviation not present in the FIPS
    mapping.
    """
    states_in_table = {
        x.start_state_fips for x in df.select("start_state_fips").distinct().collect()
    }
    codes = {x.code for x in state_fips.select("code").distinct().collect()}
    if not states_in_table.issubset(codes):
        diff = codes - states_in_table
        raise ValueError(f"dataframe has unknown state FIPS codes: {diff}")


def write_dataframe(df: DataFrame, output_dir: Path, name: str):
    """Write a dataframe to file(s) and then read it back in order to evaluate the query only once.
    Partitions data by the state column.
    """
    spark = get_spark_session()
    path = str(output_dir / (name + ".parquet"))
    logging.info("Write dataframe to %s", path)
    df.repartition("state").write.partitionBy("state").mode("overwrite").parquet(path)
    return spark.read.load(path)


def parse_args(args):
    """Parse the CLI arguments."""
    descr = """Process the INRIX Trips data to calculate travel metrics.

Examples:

$ python process_inrix.py "data/*.parquet"
$ python process_inrix.py "data/*ca*.parquet"
$ python process_inrix.py "data/*20200114.ca*.parquet" "data/*20200115.ca*.parquet"
$ python process_inrix.py data/inrix.trips.20200114.ca.c000.gz.parquet data/inrix.trips.20200114.ca.c001.gz.parquet
"""
    parser = argparse.ArgumentParser(description=descr, formatter_class=RawTextHelpFormatter)
    parser.add_argument("parquet_file_patterns", nargs="*")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="inrix-output",
        help="Output directory, defaults to inrix-output",
    )
    parser.add_argument(
        "-O",
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite output directory if it exists.",
    )
    parser.add_argument(
        "-s",
        "--state-fips-codes",
        default="state_fips_codes.csv",
        help="CSV file containing a mapping of state FIPS codes. Defaults to ./state_fips_codes",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    main()
