import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Step 0: Load environment variables
load_dotenv()

# Step 1: Load tokens and credentials
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Step 2: Convert July 2025 date range to UNIX timestamps (milliseconds)
start_date = int(datetime(2025, 7, 1, 0, 0).timestamp() * 1000)
end_date   = int(datetime(2025, 7, 31, 23, 59, 59).timestamp() * 1000)

# Step 3: Fetch filtered calls from HubSpot
calls = []
after = None

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

# Step 4: Write credentials to a temporary file
with open("service_account.json", "w") as f:
    f.write(credentials_json)

# Step 5: Authenticate with Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Step 6: Insert calls into "Calls" sheet
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1HkvNSwUatcwilCFjUGktQfqRtZE_BHsKbRt_JnU_K7Y/edit").worksheet("Calls")

rows = [["ID", "Title", "Timestamp", "Description", "OwnerID"]]
rows.extend(calls)

sheet.append_rows(rows, value_input_option="RAW")
print(f"✅ Inserted {len(calls)} calls into Calls sheet.")

# Step 7: Fetch and filter owners assigned to team ID 16450
owners = []
after = None

while True:
    url = "https://api.hubapi.com/crm/v3/owners?limit=100"
    if after:
        url += f"&after={after}"

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print("Error fetching owners:", response.text)
        break

    data = response.json()
    for owner in data.get("results", []):
        teams = owner.get("teams", [])
        if any(str(team.get("id")) == "16450" for team in teams):
            owners.append([
                owner.get("id", ""),
                owner.get("userId", ""),
                owner.get("email", ""),
                owner.get("firstName", ""),
                owner.get("lastName", ""),
                owner.get("createdAt", ""),
                owner.get("updatedAt", ""),
                owner.get("archived", ""),
                ", ".join([team.get("name", "") for team in teams]),
                ", ".join([str(team.get("id", "")) for team in teams])
            ])

    after = data.get("paging", {}).get("next", {}).get("after")
    if not after:
        break

print(f"✅ Fetched {len(owners)} owners assigned to team ID 16450.")

# Step 8: Insert owners into "Sales_team" sheet
sales_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1HkvNSwUatcwilCFjUGktQfqRtZE_BHsKbRt_JnU_K7Y/edit").worksheet("Sales_team")

owner_rows = [
    ["OwnerID", "UserID", "Email", "FirstName", "LastName", "CreatedAt", "UpdatedAt", "Archived", "TeamNames", "TeamIDs"]
]
owner_rows.extend(owners)

sales_sheet.append_rows(owner_rows, value_input_option="RAW")
print(f"✅ Inserted {len(owners)} owners into Sales_team sheet.")

# Step 9: Clean up credentials file
os.remove("service_account.json")
