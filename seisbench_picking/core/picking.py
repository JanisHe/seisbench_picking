"""
Functions that do the picking in parallel (using joblib).
"""

import os
import gc
import glob
import copy
import pathlib

import joblib
import obspy
import seisbench  # noqa

import pandas as pd

from typing import Union

from seisbench_picking.core.utils import station_and_dates
from seisbench_picking.core.waveforms import get_waveforms
from seisbench_picking.core.picking_interfaces import get_picker


def export_picks(filename: str, picklist: seisbench.util.annotations.PickList) -> None:
    """
    Converting picks from SeisBench picklist to a dictionary and save as a csv file.

    :param filename: Full pathname of csv file to save picks
    :param picklist: Input SeisBench Picklist that contains picks.
    """
    picks = {
        "id": [],
        "start_time": [],
        "peak_time": [],
        "end_time": [],
        "peak_value": [],
        "phase": [],
    }
    for pick in picklist:
        picks["id"].append(pick.trace_id)
        picks["start_time"].append(pick.start_time)
        picks["peak_time"].append(pick.peak_time)
        picks["end_time"].append(pick.end_time)
        picks["peak_value"].append(pick.peak_value)
        picks["phase"].append(pick.phase)

    # Save picks as csv
    df = pd.DataFrame(picks)
    df.to_csv(path_or_buf=filename)


def picks_postprocessing(output_pathname: str, station_wise: bool = False) -> None:
    """
    Does postprocessing of temporary pick files, i.e. reads single csv files and
    adds all together either to one file (station_wise = False) or to one file
    for each station (station_wise = True).

    :param output_pathname: Full Pathname where to store the picks
    :param station_wise: If True, the picks will be saved as a file for each station.
                         If False, only one large file that contains all picks is created.
                         Default is False
    """
    pick_files = glob.glob(os.path.join(output_pathname, "*.pick"))
    picks = {}
    for filename in pick_files:
        trace_id = pathlib.Path(filename).stem.split("_")[0]
        if trace_id not in picks.keys():  # Update picks
            picks.update(
                {
                    trace_id: {
                        "id": [],
                        "start_time": [],
                        "peak_time": [],
                        "end_time": [],
                        "peak_value": [],
                        "phase": [],
                    }
                }
            )

        # Read pick file and append values to picks
        pickfile = pd.read_csv(filepath_or_buffer=filename)
        for key in pickfile.columns.to_list():
            if "unnamed" not in key.lower():
                picks[trace_id][key] += pickfile[key].to_list()

        # Delete filename
        os.remove(path=filename)

    # Save picks
    if station_wise is True:
        for trace_id in picks.keys():
            filename = os.path.join(output_pathname, f"{trace_id}.csv")
            df = pd.DataFrame(picks[trace_id])
            df.to_csv(path_or_buf=filename)
    else:
        all_picks = {
            "id": [],
            "start_time": [],
            "peak_time": [],
            "end_time": [],
            "peak_value": [],
            "phase": [],
        }
        for trace_id in picks.keys():
            for key in picks[trace_id].keys():
                all_picks[key] += picks[trace_id][key]

        # Save all picks as pd Dataframe
        df = pd.DataFrame(all_picks)
        df.to_csv(os.path.join(output_pathname, "picks.csv"))


def _pick_waveform(
    date_station: tuple[int, int, str, str],
    sds_path: str,
    picker: seisbench.models.base.WaveformModel,
    picking_args: dict,
    starttime: obspy.UTCDateTime,
    endtime: obspy.UTCDateTime,
    output_pathname: str,
) -> None:
    """
    Picking waveform data from an obspy Stream that is created through this function.
    Only creates one single stream for a certain date.

    :param date_station: First entry of tuple is year, second one is the day of the year
                         third one the ID of the trace, and the fourth value is the channel code.
    :param sds_path: Pathname to SeisComp Data Structure (SDS)
    :param picker: Neural Network in SeisBench that picks seismic onsets
    :param picking_args: Dictionary that contains information for the picker.
                         For an example, see the parfile
    :param starttime: Start time of period for picking
    :param endtime: End time of picking period
    :param output_pathname: Full pathname to save the data
    """
    # Find network, station, and location from date_station
    try:
        network, station, location = date_station[2].split(".")
    except ValueError:
        network, station = date_station[2].split(".")
        location = ""  # Empty string for location if not given in stations.csv

    # Load waveform
    waveform = get_waveforms(
        network=network,
        station=station,
        location=location,
        channel_code=date_station[3],
        date=(date_station[0], date_station[1]),
        sds_path=sds_path,
        starttime=starttime,
        endtime=endtime,
    )

    # Pick seismic phases on waveform using SeisBench's classify method
    picks = picker.classify(waveform, **picking_args)

    # Save picks as .csv file in output_pathname
    filename = os.path.join(
        output_pathname, f"{date_station[2]}_{date_station[0]}.{date_station[1]}.pick"
    )
    export_picks(filename=filename, picklist=picks.picks)

    # Delete picks and waveform and run garbage collector to clean memory
    del picks
    del waveform
    gc.collect()


def pick_waveforms(
    dates: list,
    stations: pd.DataFrame,
    sds_path: str,
    starttime: Union[obspy.UTCDateTime, str],
    endtime: Union[obspy.UTCDateTime, str],
    output_pathname: str,
    picking_args: dict,
    workers: int = 1,
    station_wise: bool = False,
    verbose: bool = True,
) -> None:
    """
    Function to start the picking in parallel.

    :param dates: List of tuple that contains all dates for picking. Each tuple has to values, the
                  first value represents the year and the second the day of the year.
    :param stations: Dataframe that contains station information, i.e. id and channel code.
    :param sds_path: Pathname to SeisComp Data Structure (SDS)
    :param starttime: Start time of period for picking
    :param endtime: End time of picking period
    :param output_pathname: Full pathname to save the data
    :param picking_args: Dictionary that contains information for the picker.
                         For an example, see the parfile
    :param workers: Number of workers for parallelization
    :param station_wise: If True, the picks will be saved as a file for each station.
                         If False, only one large file that contains all picks is created.
                         Default is False
    :param verbose: If True, the loaded information about the picker will be printed.
                    Default is True
    """
    # Convert start- and end time
    if isinstance(starttime, str):
        starttime = obspy.UTCDateTime(starttime)
    if isinstance(endtime, str):
        endtime = obspy.UTCDateTime(endtime)

    # Combine dates and station ids to do picking in parallel
    dates_station = station_and_dates(
        dates=dates,
        stations=stations["id"].to_list(),
        channel_codes=stations["channel_code"].to_list(),
    )

    # Load picking model from picking interfaces
    if verbose:  # Summarize loaded picker settings
        print("Loaded picker settings:")
        for key, value in picking_args.items():
            print(f"{key}: {value}")

    # Copy picker args to avoid overwriting py pop
    tmp_picking_args = copy.deepcopy(picking_args)
    picker = get_picker(
        type=tmp_picking_args.pop("picker"), model_name=tmp_picking_args.pop("model")
    )

    # Pick waveforms in parallel
    joblib_pool = joblib.Parallel(n_jobs=workers)
    joblib_pool(
        joblib.delayed(_pick_waveform)(
            date_station=value,
            sds_path=sds_path,
            picker=picker,
            picking_args=tmp_picking_args,
            starttime=starttime,
            endtime=endtime,
            output_pathname=output_pathname,
        )
        for value in dates_station
    )

    # Sort picks from all daily picks
    picks_postprocessing(output_pathname=output_pathname, station_wise=station_wise)
