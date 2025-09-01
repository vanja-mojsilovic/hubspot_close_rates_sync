import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

# Step 0: Load environment variables
load_dotenv()

# Step 1: Load tokens and credentials
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("HUBSPOT_TOKEN is missing. Check your .env.")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not credentials_json:
    raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing. Check your .env.")

# Step 2: Define daily partitions for July 2025
def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def generate_daily_ranges_july_2025():
    ranges = []
    current = datetime(2025, 7, 1)
    while current.month == 7:
        next_day = current + timedelta(days=1)
        ranges.append((ms(current), ms(next_day)))
        current = next_day
    return ranges

daily_ranges = generate_daily_ranges_july_2025()

# Step 3: Fetch and filter owners assigned to team ID 16450
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
            owners.append(owner)

    after = data.get("paging", {}).get("next", {}).get("after")
    if not after:
        break

print(f"✅ Fetched {len(owners)} owners assigned to team ID 16450.")

# Step 4: Build lookup: OwnerID → (Email, FirstName, LastName)
owner_lookup = {}
sales_owner_ids = set()

for owner in owners:
    owner_id = str(owner.get("id", ""))
    email = owner.get("email", "")
    first = owner.get("firstName", "")
    last = owner.get("lastName", "")
    owner_lookup[owner_id] = (email, first, last)
    if owner_id:
        sales_owner_ids.add(owner_id)

print(f"🔎 Owners prepared for matching: {len(sales_owner_ids)}")

# Step 5: Fetch calls per daily range and count per OwnerID
call_counts = defaultdict(int)

for start_ts, end_ts in daily_ranges:
    human_start = datetime.fromtimestamp(start_ts / 1000)
    human_end = datetime.fromtimestamp(end_ts / 1000)
    print(f"📆 Processing: {human_start.date()}")

    after = None
    page_count = 0
    max_pages = 100

    while page_count < max_pages:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "hs_timestamp", "operator": "GTE", "value": start_ts},
                    {"propertyName": "hs_timestamp", "operator": "LT", "value": end_ts}
                ]
            }],
            "sorts": ["-hs_timestamp"],
            "properties": ["hubspot_owner_id"],
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
            print("❌ Error fetching calls:", response.text)
            print("Last paging token:", after)
            print("Request body:", json.dumps(body, indent=2))
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            break

        for call in results:
            owner_id = call.get("properties", {}).get("hubspot_owner_id")
            if owner_id:
                owner_id = str(owner_id)
                if owner_id in sales_owner_ids:
                    call_counts[owner_id] += 1

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        page_count += 1
        print(f"📞 Total calls so far: {sum(call_counts.values())} (Page {page_count})")

    if page_count >= max_pages and after:
        print("⚠️ Hit page cap on", human_start.date(), "— consider hourly partitioning if needed.")

# Step 6: Write credentials to a temporary file
with open("service_account.json", "w") as f:
    f.write(credentials_json)

# Step 7: Authenticate with Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Step 8: Build enriched summary
summary_rows = [["OwnerID", "Email", "FirstName", "LastName", "NumberOfCalls"]]

def sort_key(oid):
    email, first, last = owner_lookup.get(oid, ("", "", ""))
    return (last or "", first or "", oid)

for owner_id in sorted(sales_owner_ids, key=sort_key):
    email, first, last = owner_lookup.get(owner_id, ("", "", ""))
    count = call_counts.get(owner_id, 0)
    summary_rows.append([owner_id, email, first, last, count])

# Step 9: Overwrite "number_of_calls_july" sheet
summary_sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1HkvNSwUatcwilCFjUGktQfqRtZE_BHsKbRt_JnU_K7Y/edit"
).worksheet("number_of_calls_july")

summary_sheet.clear()
summary_sheet.update(values=summary_rows, range_name="A1", value_input_option="RAW")

print(f"✅ Overwrote 'number_of_calls_july' with {len(summary_rows)-1} owners "
      f"(calls found for {len(call_counts)}; zero-filled {len(sales_owner_ids) - len(call_counts)}).")

# Step 10: Clean up credentials file
os.remove("service_account.json")
