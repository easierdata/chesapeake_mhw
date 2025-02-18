#########
# This script ...
# Runtime on my local machine ~2 hours
# This script generates the file L4_GHRSST-SSTfnd-Geo_Polar_Blended_Night-GLOB-v02.0-fv01.0_CB_{START_DATE}_{END_DATE}.nc
# at the location of the output directory specified in this file
#########
import time
import earthaccess
from typing import List, Union
from pathlib import Path
import concurrent.futures
import xarray as xr

# global variables, change as needed
START_DATE = "20020901"
END_DATE = "20230831"
BBOX = (36.750000, -77.500000, 40.000000, -75.500000)
TRY_ASYNC = True
FILENAME_SUFFIX = "_CB"

# Select the dataset to download and process
DATASET = "mur"
if DATASET not in ["geopolar", "mur"]:
    raise KeyError("Valid options for DATASET are `geopolar` or `mur`")

# Assuming script is running from the notebooks directory. Need to get filepath to data
# directory located at the root of the project folder
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(REPO_ROOT, "data", "interim")
SCRATCH_DIR = Path(REPO_ROOT, "data", "scratch", DATASET)
FILEPATHS_TXT = Path(DATA_DIR, f"filepaths_{DATASET}_{START_DATE}_{END_DATE}.txt")
OUTPUT_DIR = Path(DATA_DIR, f"SST-{DATASET}-chesapeake", "v2")

# Set the concurrency limit to the number of threads to use for downloading
# and cropping the files
CONCURRENCY_LIMIT = 5

# Create all the directories that are needed if they do not exit.
if not Path.exists(SCRATCH_DIR):
    Path.mkdir(SCRATCH_DIR, exist_ok=True, parents=True)

if not Path.exists(OUTPUT_DIR):
    Path.mkdir(OUTPUT_DIR, exist_ok=True, parents=True)

# Read in the filepaths from the text file. This will be used as our whitelist to
# remove search results that are not needed.
with Path.open(
    Path(DATA_DIR, f"filepaths_{DATASET}_20020901_20230831.txt"), encoding="utf-8"
) as f:
    dl_payload_list = f.read().splitlines()

# Check that files were found
if len(dl_payload_list) == 0:
    raise Exception("No lines found in FILEPATHS.TXT")

# # Read the filepaths from the file into a list
# with open(FILEPATHS_TXT) as f:
#     filepaths = f.read().splitlines()


# Build output filepath
def create_filepath(url_string: str) -> Path:
    # Use original name, with FILENAME_SUFFIX appended
    filename = Path(url_string).name.split(".nc")[0] + f"{FILENAME_SUFFIX}.nc"
    return Path(OUTPUT_DIR, filename).resolve()


def process_file(url, cropped_file_path):
    # Check if the file already exists
    if not Path.exists(cropped_file_path):
        try:
            download_file_path = Path(SCRATCH_DIR, Path(url).name).resolve()

            # Open and subset the file
            ds = xr.open_dataset(download_file_path.as_posix())

            ds_chesapeake = ds.sel(lat=slice(36.75, 40), lon=slice(-77.5, -75.5))

            # Ensure that the variable `dt_1km_data` is encoded properly
            if "dt_1km_data" in ds_chesapeake.variables:
                ds_chesapeake["dt_1km_data"].encoding["dtype"] = "int64"

            # Save the subset
            ds_chesapeake.to_netcdf(cropped_file_path)
        except Exception as e:
            print(f"Error processing file {url}: {e}")


def process_files_in_batches(file_list, batch_size=5):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=CONCURRENCY_LIMIT
    ) as executor:
        futures = []
        for url in file_list:
            # url = file["url"]
            # cropped_file_path = file["cropped_file_path"]
            cropped_file_path = create_filepath(url)
            futures.append(executor.submit(process_file, url, cropped_file_path))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in future: {e}")


def download_datasets(
    dataset: str,
    bbox: tuple,
    start_date: str,
    end_date: str,
    save_path: Path,
    filter_list: Union[List[str], None] = None,
    filter_path: bool = True,
) -> None:
    """
    Downloads datasets from NASA Earthdata based on specified parameters.

    Parameters:
        dataset (str): The name of the dataset to download. Must be one of the keys in the collections dictionary.
        bbox (tuple): A tuple representing the bounding box for the area of interest in the format (min_lat, min_lon, max_lat, max_lon).
        start_date (str): The start date for the temporal range in the format 'YYYY-MM-DD'.
        end_date (str): The end date for the temporal range in the format 'YYYY-MM-DD'.
        save_path (Path): The local directory path where the downloaded files will be saved.
        filter_list (Union[List[str], None], optional): A list of file identifiers to filter the search results. Defaults to None.
        filter_path (bool, optional): If True, filters out files that already exist in the save_path directory. Defaults to True.

    Raises:
        ValueError: If the specified dataset is not in the collections dictionary.

    Returns:
        None
    """

    # login with your Earthdata credentials
    # ensure you have a .netrc file in your home directory with your Earthdata login credentials
    earthaccess.login()

    # Normalize the save path
    save_path = Path(save_path).resolve()

    collections = {
        "geopolar": {
            "id": "C2036877745-POCLOUD",
            "cmr_url": "https://cmr.earthdata.nasa.gov/search/concepts/C2036877745-POCLOUD.html",
        },
        "mur": {
            "id": "C1996881146-POCLOUD",
            "cmr_url": "https://cmr.earthdata.nasa.gov/search/concepts/C1996881146-POCLOUD.html",
        },
    }

    if dataset not in collections.keys():
        raise ValueError(f"Dataset must be one of {collections.keys()}")

    collection_deets = collections[dataset]

    # collection_id = "C1996881146-POCLOUD"
    # bbox = (36.750000, -77.500000, 40.000000, -75.500000)
    results = earthaccess.search_data(
        concept_id=collection_deets["id"],
        cloud_hosted=True,
        count=-1,
        bounding_box=bbox,
        temporal=(start_date, end_date),
    )

    # If filter list is provided, remove search results that are not in the filter list
    if filter_list:
        filtered_results = [
            result
            for result in results
            if any(result["meta"]["native-id"] in f_list for f_list in filter_list)
        ]
        print(f"{len(filtered_results)} files to download after whitelist comparison.")
        results = filtered_results

    # If filter path is provided, remove search results that are in the filter path
    if filter_path:
        filter_files = [file.name for file in save_path.glob("*")]
        filtered_results = [
            result
            for result in results
            if not any(result["meta"]["native-id"] in f_list for f_list in filter_files)
        ]
        print(
            f"{len(filtered_results)} files to download after checking existing files at {save_path.as_posix()}."
        )
        results = filtered_results

    if len(results) == 0:
        print("No new files to download.")
        return

    total_size = sum([result["size"] for result in results])
    print(f"Total size: {total_size:.2f} MB\n\n")
    start_time = time.time()
    print(
        f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}\n"
    )

    print(
        f"Downloading {len(results)} Granules from the {collection_deets['id']} collection."
    )
    print(f"Estimated payload size: {total_size:.2f} MB\n\n")

    earthaccess.download(
        results,
        local_path=save_path.as_posix(),
        provider="POCLOUD",
        threads=CONCURRENCY_LIMIT,
    )

    print("\n\nFinished downloading.")

    end_time = time.time()
    print(
        f"\nEnd time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}\n"
    )
    return


# Get the filenames from the filepaths. This will be used to filter the download list
file_payload_names = [f.split("/")[-1] for f in dl_payload_list]

download_datasets(
    dataset=DATASET,
    bbox=BBOX,
    start_date=START_DATE,
    end_date=END_DATE,
    save_path=SCRATCH_DIR,
    filter_list=file_payload_names,
    filter_path=True,
)

process_files_in_batches(dl_payload_list)

# After downloading all of the files roll them up to create a single netcdf
# Note: doing this manually instead of invoking open_mfdataset() to avoid using dask
# on the backend, which has in the past resulted in conflicts.
cropped_filepaths = sorted(list(Path(OUTPUT_DIR).glob(f"*{FILENAME_SUFFIX}.nc")))
all_files = [xr.open_dataset(filepath) for filepath in cropped_filepaths]
full_year = xr.concat(all_files, dim="time")

# Create the new filename and save the merged file
output_filename = None
if DATASET == "mur":
    combined_mur_filename = (
        f"L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_CB_{START_DATE}_{END_DATE}.nc"
    )
    output_filename = Path(REPO_ROOT, "data", "raw", combined_mur_filename)

if DATASET == "geopolar":
    combined_geopolar_filename = f"L4_GHRSST-SSTfnd-Geo_Polar_Blended_Night-GLOB-v02.0-fv01.0_CB_{START_DATE}_{END_DATE}.nc"
    output_filename = Path(REPO_ROOT, "data", "raw", combined_geopolar_filename)

if output_filename:
    full_year.to_netcdf(output_filename)
