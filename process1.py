import pandas as pd
from datetime import datetime, timedelta
import os
import argparse

def process(parquet, out):
    df = pd.read_parquet(parquet)
    df.sort_values(by=['unit', 'timestamp'], inplace=True)
    trip, prev_u, prev_t = 0, None, None

    for _, r in df.iterrows():
        u, t = r['unit'], datetime.strptime(r['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
        if prev_u and (u != prev_u or t - prev_t > timedelta(hours=7)):
            trip += 1
        prev_u, prev_t = u, t
        fp = os.path.join(out, f"{u}_{trip}.csv")
        os.makedirs(out, exist_ok=True)
        trip_df = pd.read_csv(fp) if os.path.exists(fp) else pd.DataFrame(columns=['latitude', 'longitude', 'timestamp'])
        current_row_df = pd.DataFrame({'latitude': [r['latitude']], 'longitude': [r['longitude']], 'timestamp': [r['timestamp']]})
        trip_df = pd.concat([trip_df, current_row_df.loc[:, current_row_df.notna().any()]], ignore_index=True)
        trip_df.to_csv(fp, index=False)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Process GPS data and extract trips.")
    p.add_argument("--to_process", help="Path to the Parquet file to be processed", required=True)
    p.add_argument("--output_dir", help="Folder to store resulting CSV files", required=True)
    args = p.parse_args()
    process(args.to_process, args.output_dir)
