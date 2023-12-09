import os
import json
import csv
from datetime import datetime
import argparse

def process_json_files(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    processed_data = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(input_dir, filename)
            
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                if 'route' in data:
                    tolls = data['route']['tolls']
                    for toll in tolls:
                        unit = filename[:4]
                        trip_id = os.path.splitext(filename)[0]
                        toll_id_start = toll["start"]["id"] if "start" in toll else ""
                        toll_id_end = toll["end"]["id"] if "end" in toll else ""
                        toll_name_start = toll["start"]["name"] if "start" in toll else ""
                        toll_name_end = toll["end"]["name"] if "end" in toll else ""
                        toll_type = toll.get("type", "")
                        entry_time = toll["start"]["timestamp_localized"] if "start" in toll else ""
                        exit_time = toll["end"]["timestamp_localized"] if "end" in toll else ""
                        tag_cost = toll.get("tagCost", "")
                        cash_cost = toll.get("cashCost", "")
                        license_plate_cost = toll.get("licensePlateCost", "")

                        processed_data.append({
                            "unit": unit,
                            "trip_id": trip_id,
                            "toll_id_start": toll_id_start,
                            "toll_id_end": toll_id_end,
                            "toll_name_start": toll_name_start,
                            "toll_name_end": toll_name_end,
                            "toll_type": toll_type,
                            "entry_time": entry_time,
                            "exit_time": exit_time,
                            "tag_cost": tag_cost,
                            "cash_cost": cash_cost,
                            "license_plate_cost": license_plate_cost
                        })

    if processed_data:
        output_file_path = os.path.join(output_dir, "transformed_data.csv")
        with open(output_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=processed_data[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(processed_data)
    else:
        print("No toll information found in the processed JSON files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process toll information from JSON files and transform it into CSV format.")
    parser.add_argument("--to_process", help="Path to the JSON responses folder from process2.py", required=True)
    parser.add_argument("--output_dir", help="Folder to store the final transformed_data.csv", required=True)
    args = parser.parse_args()

    process_json_files(args.to_process, args.output_dir)
