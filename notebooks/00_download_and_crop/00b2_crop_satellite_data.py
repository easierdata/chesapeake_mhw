#########
# This script ...
# This script generates the file L4_GHRSST-SSTfnd-Geo_Polar_Blended_Night-GLOB-v02.0-fv01.0_CB_{START_DATE}_{END_DATE}.nc
# at the location of the output directory specified in this file
#########
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
