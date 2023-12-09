import os
import requests
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import argparse

load_dotenv()
API_KEY, API_URL = os.getenv("TOLLGURU_API_KEY"), os.getenv("TOLLGURU_API_URL")
VEHICLE_TYPE, MAP_PROVIDER = "5AxlesTruck", "osrm"

def process_file(fp, out):
    url, headers = f"{API_URL}?mapProvider={MAP_PROVIDER}&vehicleType={VEHICLE_TYPE}", {'x-api-key': API_KEY, 'Content-Type': 'text/csv'}
    with open(fp, 'rb') as f:
        res = requests.post(url, data=f, headers=headers)
        out_fp = os.path.join(out, f"{os.path.splitext(os.path.basename(fp))[0]}.json")
        with open(out_fp, 'w') as o:
            o.write(res.text)

def process_csv_files(csv_folder, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    csv_paths = [os.path.join(csv_folder, f) for f in csv_files]
    with ThreadPoolExecutor() as executor:
        executor.map(lambda path: process_file(path, output_dir), csv_paths)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Send CSV files to TollGuru API and store JSON responses.")
    p.add_argument("--to_process", help="Path to the CSV folder from process1.py", required=True)
    p.add_argument("--output_dir", help="Folder to store resulting JSON files", required=True)
    a = p.parse_args()
    process_csv_files(a.to_process, a.output_dir)
