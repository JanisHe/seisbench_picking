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

    :param date:
    :param starttime:
    :param endtime:
    :return:
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
    station: str,
    network: str,
    location: str,
    channel_code: str,
    date: tuple[int, int],
    client: obspy.clients.filesystem.sds.Client,
    starttime: Optional[obspy.UTCDateTime] = None,
    endtime: Optional[obspy.UTCDateTime] = None,
) -> obspy.Stream:
    """
    Reads waveform data from obspy client.

    :param station:
    :param network:
    :param location:
    :param channel_code:
    :param date: tuple(year, day of year)
    :param client:
    :param starttime:
    :param endtime:
    :return:
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

    :param network:
    :param station:
    :param location:
    :param channel_code:
    :param date:
    :param sds_path:
    :param starttime:
    :param endtime:
    :return:
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

    :param station:
    :param network:
    :param location:
    :param channel_code:
    :param date:
    :param sds_path:
    :param starttime:
    :param endtime:
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
