"""
Contains helper functions.
"""

import os
import warnings
import datetime
import obspy


def date_list(start_date: datetime.datetime, end_date: datetime.datetime) -> list:
    """
    Creates a full date list, containing all dates between start- and
    enddate. If start_date == end_date, the returned list has length 1
    and contains just one date. Takes datetime.datetime object as input
    values. Dates are tuples, where first value is year and second value
    is julian day.

    :param start_date: datetime object of start date
    :param end_date: datetime of end date
    """

    if not isinstance(start_date, datetime.datetime):
        start_date = obspy.UTCDateTime(start_date).datetime

    if not isinstance(end_date, datetime.datetime):
        end_date = obspy.UTCDateTime(end_date).datetime

    dates = []
    date = start_date
    while date <= end_date:
        date = datetime.datetime(year=date.year, month=date.month, day=date.day)
        date_obspy = obspy.UTCDateTime(date)
        dates.append((date_obspy.year, date_obspy.julday))
        date = date + datetime.timedelta(days=1)

    return dates


def station_and_dates(dates: list, stations: list, channel_codes: list) -> list:
    """
    Combines lists of dates and stations to assign each date to each station, i.e. each entry of the returned list
    contains a tuple with (year, julian day, id of station, channel code).
    :param dates:
    :param stations:
    :param channel_codes:
    :return:
    """
    dates_stations = []
    for date in dates:
        for station, channel_code in zip(stations, channel_codes):
            dates_stations.append((date[0], date[1], station, channel_code))

    return dates_stations


def check_parameters(parameters: dict) -> dict:
    """
    Checks parameters from loaded parfile.
    Raises an error if the parameters contains errors.

    :param parameters: Dictionary that contains keys and values for picking.
    """
    if obspy.UTCDateTime(parameters["starttime"]) >= obspy.UTCDateTime(
        parameters["endtime"]
    ):
        msg = f"Start time {parameters['starttime']} is before end time {parameters['endtime']}."
        raise ValueError(msg)

    if os.path.isdir(parameters["sds_path"]) is False:
        msg = f"{parameters['sds_path']} does not exist."
        raise FileNotFoundError(msg)

    if os.path.isfile(parameters["stations"]) is False:
        msg = f"{parameters['stations']} does not exist."
        raise FileNotFoundError(msg)

    if not parameters.get("station_wise"):
        parameters["station_wise"] = False

    if parameters["workers"] > os.cpu_count():
        msg = f"Number of workers ({parameters['workers']}) is greater than available CPUs ({os.cpu_count()})."
        warnings.warn(msg)

    return parameters
