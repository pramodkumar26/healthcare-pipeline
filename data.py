
import requests, csv

url = 'https://data.cms.gov/data-api/v1/dataset/92396110-2aed-4d63-a6a2-5d6207d46a29/data'
all_data = []
offset = 0
batch = 5000

while offset < 50000:
    r = requests.get(url, params={'size': batch, 'offset': offset})
    batch_data = r.json()
    if not batch_data:
        break
    all_data.extend(batch_data)
    print(f'Fetched {len(all_data)} rows...')
    offset += batch

with open(r'C:\Projects\healthcare-pipeline\ingestion\medicare_2022.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=all_data[0].keys())
    writer.writeheader()
    writer.writerows(all_data)

print('Done:', len(all_data), 'rows saved')
