import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

def fetch_api_data(api_url, output_file="api_data_raw.json", batch_size=1000, num_records=None, restart=False):
    """
    Fetches all data from the API in chunks using $limit and $offset parameters,
    and saves each batch to a file incrementally.

    Parameters:
    - api_url (str): The base URL of the API.
    - output_file (str): Path to the JSON file to save data incrementally.
    - batch_size (int): Number of records to fetch per request (default: 1000).
    - num_records (int or None): Maximum number of records to fetch. If None, fetch all records.
    """
    offset = 0

    # Check if the output file already exists and load existing data
    if os.path.exists(output_file) and not restart:
        with open(output_file, "r") as f:
            try:
                all_data = json.load(f)
                print(f"Resuming from {len(all_data)} records in {output_file}.")
            except json.JSONDecodeError:
                print(f"{output_file} is corrupted or empty. Starting fresh.")
                all_data = []
    else:
        if restart and os.path.exists(output_file):
            print(f"Restarting and clearing existing file: {output_file}")
            os.remove(output_file)
    all_data = []

    # Calculate the starting offset based on the existing data
    offset = len(all_data)
    print(f"Starting from offset {offset}...")

    while True:
        # Add $limit and $offset parameters to the API URL
        paginated_url = f"{api_url}?$limit={batch_size}&$offset={offset}"
        print(f"Fetching records starting at offset {offset}...")

        # Fetch data from the API
        try:
            response = requests.get(paginated_url)
            response.raise_for_status()
            batch_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

        # Stop if no more data is returned
        if not batch_data:
            print("No more data to fetch.")
            break

        # Append the batch to the combined data list
        all_data.extend(batch_data)

        # Save the updated data to the output file incrementally
        with open(output_file, "w") as f:
            json.dump(all_data, f, indent=2)
        print(f"Appended {len(batch_data)} records. Total records saved: {len(all_data)}")

        # Update offset to fetch the next batch
        offset += batch_size

        # Stop if a specific number of records is requested and reached
        if num_records is not None and len(all_data) >= num_records:
            print(f"Reached the specified number of records: {num_records}.")
            break

        # Break if the batch size is less than the limit, indicating the end of the dataset
        if len(batch_data) < batch_size:
            print("Reached the end of the dataset.")
            break

    print(f"Fetched a total of {len(all_data)} records. Data saved to {output_file}.")
    return all_data