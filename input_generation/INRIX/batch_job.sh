#!/bin/bash
#SBATCH --account=cscdav
#SBATCH --job-name=inrix
#SBATCH --time=01:00:00
#SBATCH --output=output_%j.o
#SBATCH --error=output_%j.e
#SBATCH --nodes=1
#SBATCH --partition=debug
#SBATCH --mem=730G

module load singularity-container

# These scripts come from https://github.com/NREL/HPC/tree/master/applications/spark
# The Singularity container used here is on Eagle at /datasets/images/apache_spark/spark341_py311.sif
SCRIPT_DIR=~/repos/HPC/applications/spark/spark_scripts
${SCRIPT_DIR}/configure_spark.sh
${SCRIPT_DIR}/start_spark_cluster.sh
singularity run instance://spark spark-submit --master spark://$(hostname):7077 process_inrix_trips.py \"data/*.parquet\"
${SCRIPT_DIR}/stop_spark_cluster.sh
