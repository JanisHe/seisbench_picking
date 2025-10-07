# Picking Datasets with SeisBench Picking Models
## Installation
Download this repository to work in the project or install this project by running
`pip install git+https://github.com/JanisHe/seisbench_picking.git`

## Requirements
`numpy`, `pandas`, `seisbench`, `obspy`, `joblib`

## Preparing Input Files
### Stations
A `.csv` file that contains station information (note the file header!):

| id         | channel_code |
|------------|--------------|
| FO.BETS.00 | HH           |
| RG.RITT.00 | HH           |
| RG.KUHL.00 | EH           |
| FO.OPS.00  | BH           |
| FO.OPS.00  | HH           |

- `id`: ID of each station (`network.station.location`)
- `channel_code`: Channel code of each station. Note if you have different channel code, you can add a new line with a second channel code

### Parameter File / Dictionary
In `parfiles/parfile.yml` you can find an example for settings. This parameter file is loaded
in `main`, however instead of the parameter file the function `main` also takes a dictionary
as input. Either the dictionary or the parameter file must contain the following parameters/keys:
- starttime: `str` of the start time, e.g. "2020-01-05"
- endtime: `str`of the end time, e.g "2020-01-10 05:00"
- sds_path: `str` of the pathname to read the waveform data from a SeisComp Data Structure (SDS)
- output_pathname: `str` where the picking results are saved
- workers: `int` of numbers of CPUs for parallelization
- picking:
  - picker: `str` which [SeisBench picker](https://seisbench.readthedocs.io/en/stable/pages/models.html#overview) is used
  - model: `str` which trained model is used for picking

More parameters, including optional ones are mentioned in the example `parfile`.

## Start picking of dataset
To start the picking, you use the following code:
```
from seisbench_picking.core import main

parfile = "/Path/to/my/parfile.yml"  # or define a dictionary

# Dictionary instead of parfile
parameters = {
    "starttime": ...,
    "endtime": ...,
    ....
    "picking": {
        "picker": ...,
        "model": ...,
        ...
        }
    }

# Run main for parfile
main(parfile=parfile)
# Run main for parameter dict
main(parfile=parameters)
```

In case you get an `ModuleNotFoundError`, you can add the `PythonPath` to your script by
```
import sys
sys.path.append("/Path/to/my/SeisBench/picking/project")
```

## Results
If you read in the settings from a `parfile`, the `parfile`and `stations.csv` file are
copied to `output_pathname`. The picks will be saved in a single file called `picks.csv`,
containing the following structure:

| id         | start_time                  | peak_time                   | end_time                    | peak_value  | phase |
|------------|-----------------------------|-----------------------------|-----------------------------|-------------|-------|
| TL.TL08.00 | 2020-04-01T00:06:05.720000Z | 2020-04-01T00:06:05.840000Z | 2020-04-01T00:06:05.960000Z | 0.3475416   | P     |
| TL.TL08.00 | 2020-04-01T00:06:46.030000Z | 2020-04-01T00:06:46.190000Z | 2020-04-01T00:06:46.430000Z | 0.605832    | S     |
| TL.TL08.00 | 2020-04-01T00:06:46.200000Z | 2020-04-01T00:06:46.230000Z | 2020-04-01T00:06:46.610000Z | 0.33495384  | P     |
| TL.TL08.00 | 2020-04-01T00:06:50.450000Z | 2020-04-01T00:06:50.450000Z | 2020-04-01T00:06:50.510000Z | 0.3267983   | P     |

- `id`: ID of each station (`network.station.location`)
- `start_time`: Start time of the pick
- `peak_time`: Time of pick with the highest probability value (i.e. most probable arrival time)
- `end_time`: End time of the pick
- `peak_value`: Output probability from the loaded picker model
- `phase`: Phase type of the pick, either `P` or `S`

## Additional notes
To keep memory low, it is recommended to pick only for single days. This means if you want
to analyse a large dataset, use a for loop and define a new start- and end time in each
iteration. Do not forget to copy your results from the `output_pathname`, otherwise your
results will be overwritten in each iteration.

## Next steps
One a file with all picks is created, the next step is to build from the picks an
earthquake catalog by associating the picks. This is done by `seismic phase associators`.
[Here](https://github.com/JanisHe/association) you can find a package which does the
association using three different seismic phase associators.
