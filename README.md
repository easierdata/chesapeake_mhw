# Spatial variability of marine heatwaves in the Chesapeake Bay

<!-- [![Build Status](https://github.com/rwegener2/chesapeake_mhw/workflows/Tests/badge.svg)](https://github.com/rwegener2/chesapeake_mhw/actions) -->
[![codecov](https://codecov.io/gh/rwegener2/chesapeake_mhw/branch/main/graph/badge.svg)](https://codecov.io/gh/rwegener2/chesapeake_mhw)
[![License:BSD-3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-lightgray.svg?style=flt-square)](https://opensource.org/licenses/BSD-3-Clause)
[![pypi](https://img.shields.io/pypi/v/chesapeake_mhw.svg)](https://pypi.org/project/chesapeake_mhw)
[![Documentation Status](https://readthedocs.org/projects/chesapeake_mhw/badge/?version=latest)](https://chesapeake_mhw.readthedocs.io/en/latest/?badge=latest)
[![DOI](https://zenodo.org/badge/DOI/10.31223/X5299J.svg)](https://doi.org/10.31223/X5299J)
<!-- [![conda-forge](https://img.shields.io/conda/dn/conda-forge/chesapeake_mhw?label=conda-forge)](https://anaconda.org/conda-forge/chesapeake_mhw) -->

## Authors

Rachel Wegener , Jacob O. Wenegrat, Veronica Lance, Skylar Lama

### Environment Setup

Use poetry to install the dependencies. This can be done by running the following command in the root directory of the repository:

```bash
poetry install
```

A virtual environment will be created at this root of this repo and the dependencies will be installed.

Activate the environment by running:

```bash
poetry shell
```

Install the [IPFS desktop](https://docs.ipfs.tech/how-to/desktop-app/) app or [Kubo CLI client](https://docs.ipfs.tech/install/command-line/) as this will will allow you to run a local IPFS node on your machine.

### Reproducing the pipeline

Overview of Steps:

1. Download the datasets: geopolar, nasa mur, Chesepeake Bay program in situ data from IPFS using the scripts provided. There should then be three datasets in the `data/raw` folder after data has been processed.
2. Run the notebooks in the `notebooks` folder. This generates the figures.

### Organization

#### `data` Folder

Folders created to hold data throughout the processing pipeline, from raw data to fully processed data. The folders are empty on github, but the data can be processed running the following scripts:

1. [`00b1_download_from_ipfs.py`](./notebooks/00_download_and_crop/00b1_download_from_ipfs.py) - Downloads the data from IPFS and saves it to the directory `./data/scratch`
   - Specify the collection to be downloaded by changing the `DATASET` variable in the script.  Valid options for DATASET are `geopolar`, `mur`, or `cbp`
2. [`00b2_crop_satellite_data.py`](./notebooks/00_download_and_crop/00b2_crop_satellite_data.py) - Crops the satellite data to the Chesapeake Bay region and saves it to the directory `./data/interim`. The images are then merged into a single `.netcdf` file and saved to `./data/raw`.
   - Specify the collection to be downloaded by changing the `DATASET` variable in the script.  Valid options for DATASET are `geopolar` or `mur`.

#### `figures` Folder

Contains the final figures, as generated in the data analysis notebooks

### `notebooks` Folder

Jupyter notebooks written in Python with instructions for processing the data from the `raw` format through the generation of relevant `figures`.
