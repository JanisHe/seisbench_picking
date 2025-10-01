import os
import sys
import yaml
import time
import shutil

import pandas as pd

from typing import Union

from obspy import UTCDateTime

from seisbench_picking.core.utils import date_list, check_parameters
from seisbench_picking.core.picking import pick_waveforms


def main(parfile: Union[str, dict]):
    """

    :param parfile:
    :return:
    """
    # Read parameter file
    if isinstance(parfile, str):
        with open(parfile, "r") as f:
            parameters = yaml.safe_load(f)
    elif isinstance(parfile, dict):
        parameters = parfile
    else:
        msg = f"parfile must be either of type 'str' or 'dict' but is of type {type(parfile)}."
        raise ValueError(msg)

    # Check all parameters in parfile
    parameters = check_parameters(parameters=parameters)

    # Create directory to save output and copy parameter yml file and station file
    if not os.path.isdir(parameters["output_pathname"]):
        os.makedirs(parameters["output_pathname"])
    try:
        if isinstance(parfile, str):
            shutil.copyfile(
                src=parfile,
                dst=os.path.join(parameters["output_pathname"], "parfile.yml"),
            )
            shutil.copyfile(
                src=parameters["stations"],
                dst=os.path.join(parameters["output_pathname"], "stations.csv"),
            )
    except shutil.SameFileError as e:
        print(e)
        print("Keeping old file and do not overwrite")

    # Read stations and create list with all dates (i.e. tuple of year and julian day)
    stations = pd.read_csv(filepath_or_buffer=parameters["stations"])
    dates = date_list(
        start_date=UTCDateTime(parameters["starttime"]).datetime,
        end_date=UTCDateTime(parameters["endtime"]).datetime,
    )

    # Start picking
    pick_stime = time.time()
    pick_waveforms(
        dates=dates,
        stations=stations,
        sds_path=parameters["sds_path"],
        starttime=parameters["starttime"],
        endtime=parameters["endtime"],
        output_pathname=parameters["output_pathname"],
        picking_args=parameters["picking"],
        workers=parameters["workers"],
        station_wise=parameters["station_wise"],
    )

    print(f"Finished picking after {time.time() - pick_stime:.2f} s.")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        parfile = "../../parfiles/parfile.yml"
    elif len(sys.argv) > 1 and os.path.isfile(sys.argv[1]) is False:
        msg = "The given file {} does not exist. Perhaps take the full path of the file.".format(
            sys.argv[1]
        )
        raise FileNotFoundError(msg)
    else:
        parfile = sys.argv[1]

    # Start to pick phases from parfile
    main(parfile=parfile)
