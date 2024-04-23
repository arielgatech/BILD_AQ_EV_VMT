import argparse
import itertools
import logging
import sys
from argparse import RawTextHelpFormatter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from utils import setup_logging

logger = logging.getLogger(__name__)


def main():
    args = parse_args(sys.argv[1:])
    make_plots(Path(args.output_dir), args.states, args.max_workers)


def make_plots(output_dir: Path, states, max_workers):
    setup_logging(output_dir / "inrix_plots.log")
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    data_dir = output_dir / "cleaned.parquet"
    if not data_dir.exists():
        raise FileNotFoundError(f"Expected {data_dir.name} in {output_dir}")

    if not states:
        states = [x.name.split("=")[1] for x in data_dir.iterdir() if x.name.startswith("state=")]
        if not states:
            raise FileNotFoundError(f"Expected state directories in {data_dir} in the format 'state=XX'")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for _ in executor.map(make_state_plots, states, itertools.repeat(data_dir), itertools.repeat(plots_dir)):
            pass


def make_state_plots(state: str, data_dir: Path, output_dir: Path):
    df = pd.read_parquet(data_dir / f"state={state}")
    #columns = ["trip_id", "o_GEOID", "d_GEOID", "travel_time_h", "trip_distance_mile"]
    sample = df[df["vehicle_weight_class"] == 1][["travel_time_h"]]
    #plot_travel_time_histogram(sample, state, output_dir)
    plot_fitted_travel_time_curve(sample, state, output_dir)


def plot_travel_time_histogram(df: pd.DataFrame, state: str, output_dir: Path):
    """Plot a histogram ..."""
    df["travel_time_h"].plot(kind="hist", bins=500)
    plt.xlabel("travel time (h)")
    plt.xlim([0, 2])
    filename = output_dir / f"{state}_travel_time.png"
    plt.savefig(filename, dpi=300)
    logging.info("Created travel-time plot in %s", filename)
    plt.cla()
    plt.clf()


def plot_fitted_travel_time_curve(df: pd.DataFrame, state: str, output_dir: Path):
    params = stats.exponweib.fit(df["travel_time_h"], floc=0, f0=1)
    shape = params[1]
    scale = params[3]
    _, bins, _ = plt.hist(df["travel_time_h"], bins=50, range=(0, 2), density=True)
    center = (bins[:-1] + bins[1:]) / 2.0

    # Using all params and the stats function
    legend_text = "shape = {a}, scale = {b}".format(
        a=np.round(shape, 3), b=np.round(scale, 3)
    )
    plt.plot(center, stats.exponweib.pdf(center, *params), lw=4, label=legend_text)
    # plt.plot(center,stats.exponweib.sf(center,*params),lw=4,label="survival function")
    plt.xlabel("travel time (h)")
    plt.legend()
    filename = output_dir / f"{state}_fitted_travel_time_curve.png"
    plt.savefig(filename, dpi=300)
    logging.info("Created fitted travel-time plot in %s", filename)
    plt.cla()
    plt.clf()


def parse_args(args):
    """Parse the CLI arguments."""
    descr = """Plot metrics for INRIX Trips data generated with process_inrix_trips.py.
By default, create one plot per state for all states in parallel by using all CPUs in the system.
Optionally, pass state abbreviations as arguments.

Examples:

$ python plot_trip_metrics.py CA
$ python plot_trip_metrics.py -o inrix-output CA OR WA
"""
    parser = argparse.ArgumentParser(description=descr, formatter_class=RawTextHelpFormatter)
    parser.add_argument("states", nargs="*")
    parser.add_argument(
        "-m",
        "--max-workers",
        type=int,
        help="Max workers to run in parallel, defaults to number of CPUs. Use a lesser value if "
        "the default will consume all system memory.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="inrix-output",
        help="Output directory, defaults to inrix-output",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    main()
