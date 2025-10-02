"""
Functions to read waveforms from SDS structure.
"""

import os
import warnings
import datetime
import obspy

from typing import Optional
from obspy.clients.filesystem.sds import Client


def start_and_endtime(
    date: tuple[int, int],
    starttime: Optional[obspy.UTCDateTime] = None,
    endtime: Optional[obspy.UTCDateTime] = None,
) -> (obspy.UTCDateTime, obspy.UTCDateTime):
    """
    Checks from given date (year, day of year) whether start- and end time of stream
    needs to be modified, for example at the beginning and end of the picking period.
    It returns a start- and end time to read data using either an obspy client or
    the read function from obspy.

    :param date: Tuple (year, day of year)
    :param starttime: Start time of picking period
    :param endtime: End time of picking period
    """
    # Get start- and end time from julian day in date
    date_from_doy = datetime.datetime(
        year=date[0], month=1, day=1
    ) + datetime.timedelta(days=date[1] - 1)

    # Obtain start- and end time of stream
    if starttime and starttime.year == date[0] and starttime.julday == date[1]:
        starttime_stream = starttime
    else:
        starttime_stream = obspy.UTCDateTime(date_from_doy)

    if endtime and endtime.year == date[0] and endtime.julday == date[1]:
        endtime_stream = endtime
    else:
        endtime_stream = (
            obspy.UTCDateTime(date_from_doy + datetime.timedelta(days=1)) - 1e-6
        )  # Obtain last microsecond of previous day, i.e. day of start time

    return starttime_stream, endtime_stream


def get_waveforms_client(
    network: str,
    station: str,
    location: str,
    channel_code: str,
    date: tuple[int, int],
    client: obspy.clients.filesystem.sds.Client,
    starttime: Optional[obspy.UTCDateTime] = None,
    endtime: Optional[obspy.UTCDateTime] = None,
) -> obspy.Stream:
    """
    Reads waveform data from a given obspy SDS client.
    The function returns an obspy Stream. If no data are
    found, the stream does not contain any trace.
    Gaps are filled by zeros (stream.merge(fill_value=0)).

    :param network: Name of seismic network
    :param station: Name of seismic station
    :param location: Location code of seismic station
    :param channel_code: Channel code of seismic station, e.g. HH, EH, BH, ...
    :param date: Information about the date as a tuple (year, day of year)
    :param client: Obspy SDS client
    :param starttime: Start time of the picking period
    :param endtime: End time of the picking period
    """
    # Get start- and end time from julian day in date
    starttime_stream, endtime_stream = start_and_endtime(
        date=date, starttime=starttime, endtime=endtime
    )

    # Read waveform data using obspy client
    try:
        stream = client.get_waveforms(
            network=network,
            station=station,
            location=location,
            channel=f"{channel_code}*",
            starttime=starttime_stream,
            endtime=endtime_stream,
        )
    except ValueError:
        return obspy.Stream()

    # Merge stream and fill gaps with zeros
    stream.merge(fill_value=0)

    return stream


def get_waveforms_sds_path(
    network: str,
    station: str,
    location: str,
    channel_code: str,
    date: tuple[int, int],
    sds_path: str,
    starttime: Optional[obspy.UTCDateTime] = None,
    endtime: Optional[obspy.UTCDateTime] = None,
) -> obspy.Stream:
    """
    Backup function if reading waveform data from obspy SDS client does not work.
    In that case, this function tries to create an own pathname to read the
    seismic data from the SDS path. If no data are found, the stream contains
    no data. Gaps are filled by zeros (stream.merge(fill_value=0)).

    :param network: Name of seismic network
    :param station: Name of seismic station
    :param location: Location code of seismic station
    :param channel_code: Channel code of seismic station, e.g. HH, EH, BH, ...
    :param date: Information about the date as a tuple (year, day of year)
    :param sds_path: Pathname of SDS (SeisComp Data Structure)
    :param starttime: Start time of the picking period
    :param endtime: End time of the picking period
    """
    if not os.path.isdir(sds_path):
        msg = f"Pathname {sds_path} to read waveform data does not exist."
        raise IOError(msg)

    sds_pathname = os.path.join(
        "{sds_path}",
        "{year}",
        "{network}",
        "{station}",
        "{channel}*",
        "{network}.{station}.{location}.{channel}*{julday}",
    )
    pathname = sds_pathname.format(
        sds_path=sds_path,
        year=date[0],
        network=network,
        station=station,
        channel=channel_code,
        location=location,
        julday="{:03d}".format(
            date[1]
        ),  # Format julian day as string with three characters
    )

    starttime_stream, endtime_stream = start_and_endtime(
        date=date, starttime=starttime, endtime=endtime
    )

    # Read waveform data and fill gaps with zeros
    try:
        stream = obspy.read(
            pathname_or_url=pathname, starttime=starttime_stream, endtime=endtime_stream
        )
        stream.merge(fill_value=0)
    except Exception:
        stream = obspy.Stream()  # Return empty stream if no data are found

    return stream


def get_waveforms(
    station: str,
    network: str,
    location: str,
    channel_code: str,
    date: tuple[int, int],
    sds_path: str,
    starttime: Optional[obspy.UTCDateTime] = None,
    endtime: Optional[obspy.UTCDateTime] = None,
):
    """
    Main function to read seismic data from a given SeisComp Data Structure (SDS)
    pathname. First the function tries to use an obspy sds client. If no data are
    found, a second function tries to read the data from the SDS path.
    The function returns an obspy stream, which has no traces if no data were found.
    Gaps are filled by zeros (stream.merge(fill_value=0)).

    :param station: Name of seismic network
    :param network: Name of seismic station
    :param location: Location code of seismic station
    :param channel_code: Channel code of seismic station, e.g. HH, EH, BH, ...
    :param date: Information about the date as a tuple (year, day of year)
    :param sds_path: Pathname of SDS (SeisComp Data Structure)
    :param starttime: Start time of the picking period
    :param endtime: End time of the picking period
    :return:
    """
    # Try to read data from obspy client
    client = Client(sds_root=sds_path)
    stream = get_waveforms_client(
        network=network,
        station=station,
        location=location,
        channel_code=channel_code,
        date=date,
        client=client,
        starttime=starttime,
        endtime=endtime,
    )

    # If stream has no data (i.e. len(stream) == 0), try to read data from sds path
    if len(stream) == 0:
        stream = get_waveforms_sds_path(
            network=network,
            station=station,
            location=location,
            channel_code=channel_code,
            date=date,
            sds_path=sds_path,
            starttime=starttime,
            endtime=endtime,
        )

    # Print warning if no data were found
    if len(stream) == 0:
        msg = (
            f"No data for {network}.{station}.{location}.{channel_code}* were found on year={date[0]} and day of "
            f"year={date[1]}."
        )
        warnings.warn(msg)

    return stream
