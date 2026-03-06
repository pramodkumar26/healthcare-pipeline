import os
import requests
import csv
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "healthcare-pipeline-489402"
BUCKET_NAME = "healthcare-pipeline-raw-data"
LOCAL_FILE = r"C:\Projects\healthcare-pipeline\ingestion\medicare_2022.csv"
GCS_DESTINATION = "raw/medicare_2022.csv"
API_URL = "https://data.cms.gov/data-api/v1/dataset/92396110-2aed-4d63-a6a2-5d6207d46a29/data"

def fetch_cms_data(total_rows=50000, batch_size=5000):
    all_data = []
    offset = 0
    while offset < total_rows:
        response = requests.get(API_URL, params={"size": batch_size, "offset": offset})
        batch = response.json()
        if not batch:
            break
        all_data.extend(batch)
        print(f"Fetched {len(all_data)} rows...")
        offset += batch_size
    return all_data

def save_locally(data, filepath):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved locally: {filepath}")

def upload_to_gcs(local_path, bucket_name, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{destination}")

if __name__ == "__main__":
    data = fetch_cms_data()
    save_locally(data, LOCAL_FILE)
    upload_to_gcs(LOCAL_FILE, BUCKET_NAME, GCS_DESTINATION)
    print("Ingestion complete.")