from calendar import c
from ipfs_stac import client
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# Select the dataset to download and process
DATASET = "mur"
if DATASET not in ["geopolar", "mur", "cbp"]:
    raise KeyError("Valid options for DATASET are `geopolar`, `mur`, or `cbp`")

# creat a web3 client object so we retrieve content from IPFS.
# NOTE: by default we must pass in a STAC endpoint to the client object
client_obj = client.Web3(stac_endpoint="https://cmr.earthdata.nasa.gov/stac/")

# Import a list of file CIDs as to retrieve from IPFS
# get root directory based on the location of this script
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(REPO_ROOT, "data")
payload_file = Path(DATA_DIR, "cid_payload.csv")
cid_df = pd.read_csv(payload_file)

# Filter dataframe to the currently selected dataset collection name
cid_df = cid_df[cid_df["collection_name"] == DATASET]

# CSV should contain the following fields: filename, collection_name, cid

# Retrieve content and save to disk.
SCRATCH_DIR = Path(DATA_DIR, "scratch")
if not Path.exists(SCRATCH_DIR):
    Path.mkdir(SCRATCH_DIR, exist_ok=True)

for _, row in tqdm(cid_df.iterrows()):
    cid = row["cid"]
    filename = row["filename"]
    output_directory = Path(SCRATCH_DIR, DATASET)
    if not Path.exists(output_directory):
        Path.mkdir(output_directory, exist_ok=True)

    if Path.exists(Path(output_directory, filename)):
        print(f"{filename} already exists.")
        continue

    print(f"Retrieving..... {filename} ")
    try:
        client_obj.writeCID(cid, Path(output_directory, filename))
    except Exception as e:
        print(f"Failed to retrieve {filename}: {e}")
        continue

if DATASET == "cbp":
    # Combine all the CSVs found in the cbp directory

    start_date = datetime(2003, 1, 1)
    end_date = datetime(2022, 12, 31)

    # Directory containing the Chesapeake Bay Program Water Quality Dataset
    cbp_dir = Path(SCRATCH_DIR, "cbp")

    full_df = pd.concat([pd.read_csv(file) for file in cbp_dir.glob("*.csv")])

    # Sort by date and reset the index
    full_df.SampleDate = pd.to_datetime(full_df.SampleDate)
    full_df = full_df.sort_values("SampleDate").reset_index(drop=True)

    # Remove rows with null temperature values
    full_df = full_df[~full_df.MeasureValue.isnull()]

    # Save to the raw data folder
    filename = ("WaterQuality_ChesapeakeBayProgram_{}_{}_TempDOSal.csv").format(
        start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    )

    # check if the folder exists, if not create it
    if not Path.exists(Path(REPO_ROOT, "data", "raw")):
        Path.mkdir(Path(REPO_ROOT, "data", "raw"))

    output_path = Path(REPO_ROOT, "data", "raw", filename)

    full_df.to_csv(output_path, index=False)
