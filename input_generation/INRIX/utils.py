import logging
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark import SparkConf


def init_spark(name="inrix"):
    """Initialize a SparkSession.

    Parameters
    ----------
    name : str
    """
    conf = SparkConf().setAppName(name)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    log_spark_conf(spark)
    return spark


def get_spark_session() -> SparkSession:
    """Return the active SparkSession or create a new one is none is active."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        logging.warning("Could not find a SparkSession. Create a new one.")
        spark = SparkSession.builder.getOrCreate()
        log_spark_conf(spark)
    return spark


def log_spark_conf(spark: SparkSession):
    """Log the Spark configuration details."""
    conf = spark.sparkContext.getConf().getAll()
    conf.sort(key=lambda x: x[0])
    logging.info("Spark conf: %s", "\n".join([f"{x} = {y}" for x, y in conf]))


@contextmanager
def custom_spark_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.
    """
    spark = get_spark_session()
    orig_settings = {}

    try:
        for key, val in conf.items():
            orig_settings[key] = spark.conf.get(key)
            spark.conf.set(key, val)
            logging.info("Set %s=%s temporarily", key, val)
        yield
    finally:
        for key, val in orig_settings.items():
            spark.conf.set(key, val)


def setup_logging(filename):
    """Setup logging for console and file."""
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)
