# INRIX Trips Data Processing

The scripts in this repository implement filtering and aggregation for the INRIX Trips dataset from
https://livewire.energy.gov/.

The queries use Apache Spark because of its scalabale compute capabilities. A query executed on a
Spark cluster with 5 compute nodes will run approximately 5 times faster than on a cluster with
one node.

## Data
Download your desired trip data from https://livewire.energy.gov/. This data used for this project
is currently at `/projects/cscdav/inrixdata/data`.

## Installation
Create a Python 3.11 virtual environment with your preferred tool. If you are not familiar with Python
virtual environments, install ``Miniconda`` (not ``Anaconda``) by following instructions at
https://conda.io/projects/conda/en/stable/user-guide/install/

The version of pyspark used here is 3.4.1. This needs to match the version of the Spark cluster.
Adjust as necessary.

```
$ conda create -y -n inrix python=3.11
$ conda activate inrix
$ pip install -r requirements.txt
```

## Apache Spark
The scripts require an Apache Spark cluster. You can provision the cluster however you'd like.
The cloud providers offer services to do this.

For this project we created ephemeral Spark clusters on HPC compute nodes with scripts from
https://github.com/NREL/HPC/tree/master/applications/spark.

The overall process is as follows:

- Create a Singularity container with Apache Spark software. This is on Eagle at
`/datasets/images/apache_spark/spark341_py311.sif`.
- Acquire one or more compute nodes with Slurm. Choose nodes with fast local storage. On Eagle this
means `bigmem` or `gpu` nodes. Use 1 - 3 nodes dependening on node availability and
how fast you want the queries to run.
- Run the helper scripts to start Spark services on each compute node and create a cluster.
- Run the scripts in this repository to filter and aggregrate the data with the cluster.

## Scripts

### Process trip data
You can run `process_inrix_trips.py` by itself to use Spark in local mode. This will work on small
datasets and is good for test purposes. Use a Spark cluster for regular datasets.

To run the script on a cluster, run it through `spark-submit` like this example:

**Note**: Run this from the first node in your Slurm allocation because that is where the Spark
master process is running.

```
$ spark-submit --master spark://$(hostname):7077 process_inrix_trips.py data/*.parquet
```

The script `batch_job.sh` provides a full example of how to submit a job to the Slurm queue that
creates and configures the Spark cluster and then runs the processing script.

Here is documentation for the script:

```
python process_inrix_trips.py --help                 
usage: process_inrix_trips.py [-h] [-o OUTPUT_DIR] [-O] [-s STATE_FIPS_CODES] [parquet_file_patterns ...]

Process the INRIX Trips data to calculate travel metrics.

Examples:

$ python process_inrix.py "data/*.parquet"
$ python process_inrix.py "data/*ca*.parquet"
$ python process_inrix.py "data/*20200114.ca*.parquet" "data/*20200115.ca*.parquet"
$ python process_inrix.py data/inrix.trips.20200114.ca.c000.gz.parquet data/inrix.trips.20200114.ca.c001.gz.parquet

positional arguments:
  parquet_file_patterns

options:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory, defaults to inrix-output
  -O, --overwrite       Overwrite output directory if it exists.
  -s STATE_FIPS_CODES, --state-fips-codes STATE_FIPS_CODES
                        CSV file containing a mapping of state FIPS codes. Defaults to ./state_fips_codes
```

### Make plots
Run this script to make plots on a compute node. Do not use the login node because the script will
consume all CPUs.

Here is the documentation:

```
python plot_trip_metrics.py --help  
usage: plot_trip_metrics.py [-h] [-m MAX_WORKERS] [-o OUTPUT_DIR] [states ...]

Plot metrics for INRIX Trips data generated with process_inrix_trips.py.
By default, create one plot per state for all states in parallel by using all CPUs in the system.
Optionally, pass state abbreviations as arguments.

Examples:

$ python plot_trip_metrics.py CA
$ python plot_trip_metrics.py -o inrix-output CA OR WA

positional arguments:
  states

options:
  -h, --help            show this help message and exit
  -m MAX_WORKERS, --max-workers MAX_WORKERS
                        Max workers to run in parallel, defaults to number of CPUs. Use a lesser value if the default will consume all system memory.
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory, defaults to inrix-output
```
