import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Step 1: Load token from .env
load_dotenv()
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# Step 2: Convert July 2025 date range to UNIX timestamps (milliseconds)
start_date = int(datetime(2025, 7, 1, 0, 0).timestamp() * 1000)
end_date   = int(datetime(2025, 7, 31, 23, 59, 59).timestamp() * 1000)

# Step 3: Initialize variables
calls = []
after = None

# Step 4: Fetch filtered calls
while len(calls) < 10000:
    body = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "hs_timestamp", "operator": "GT", "value": str(start_date)},
                {"propertyName": "hs_timestamp", "operator": "LT", "value": str(end_date)},
                {"propertyName": "hubspot_owner_id", "operator": "IN", "values": ["80955236", "80955235", "38309709"]}
            ]
        }],
        "sorts": ["-hs_timestamp"],
        "properties": [
            "hs_call_title",
            "hs_call_body",
            "hs_timestamp",
            "hubspot_owner_id"
        ],
        "limit": 100
    }
    if after:
        body["after"] = after

    response = requests.post(
        "https://api.hubapi.com/crm/v3/objects/calls/search",
        headers=HEADERS,
        data=json.dumps(body)
    )

    if response.status_code != 200:
        print("Error:", response.text)
        break

    data = response.json()
    for call in data.get("results", []):
        calls.append([
            call["id"],
            call["properties"].get("hs_call_title", ""),
            call["properties"].get("hs_timestamp", ""),
            call["properties"].get("hs_call_body", ""),
            call["properties"].get("hubspot_owner_id", "")
        ])

    after = data.get("paging", {}).get("next", {}).get("after")
    if not after:
        break

    print(f"Fetched {len(calls)} filtered July calls so far...")

# Step 5: Export to timestamped CSV
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
filename = f"Calls in July 2025 filtered by owner {timestamp}.csv"
filepath = os.path.join(os.path.expanduser("~"), "Downloads", filename)

import csv
with open(filepath, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["ID", "Title", "Timestamp", "Description", "OwnerID"])
    writer.writerows(calls)

print(f"Exported {len(calls)} calls to {filepath}")
