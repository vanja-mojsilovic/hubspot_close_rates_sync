import os
import requests
import json
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

# Optional dotenv support for local runs
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not needed in GitHub Actions

# Step 1: Load tokens and credentials
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("HUBSPOT_TOKEN is missing. Check your .env or GitHub Actions secrets.")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not credentials_json:
    raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing. Check your .env or GitHub Actions secrets.")

# Step 2: Define daily partitions for August 2025
def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def generate_daily_ranges_aug_2025():
    ranges = []
    current = datetime(2025, 8, 1)
    while current.month == 8:
        next_day = current + timedelta(days=1)
        ranges.append((ms(current), ms(next_day)))
        current = next_day
    return ranges

daily_ranges = generate_daily_ranges_aug_2025()

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
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            break

        for call in results:
            owner_id = call.get("properties", {}).get("hubspot_owner_id")
            if owner_id and str(owner_id) in sales_owner_ids:
                call_counts[str(owner_id)] += 1

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        page_count += 1

# Step 5b: Fetch meetings per daily range and count per OwnerID
meeting_counts = defaultdict(int)
allowed_types = ["New Demo Meeting", "Sales Meeting Scheduled - Pitch/Demo"]

for start_ts, end_ts in daily_ranges:
    offset = 0
    page_count = 0
    max_pages = 100

    while page_count < max_pages:
        url = f"https://api.hubapi.com/engagements/v1/engagements/paged?limit=100&offset={offset}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print("❌ Error fetching meetings:", response.text)
            break

        data = response.json()
        engagements = data.get("results", [])
        if not engagements:
            break

        for eng in engagements:
            if eng.get("engagement", {}).get("type") != "MEETING":
                continue

            ts = eng["engagement"].get("timestamp", 0)
            if not (start_ts <= ts < end_ts):
                continue

            owner_id = str(eng["engagement"].get("ownerId", ""))
            if owner_id not in sales_owner_ids:
                continue

            metadata = eng.get("metadata", {})
            meeting_type = metadata.get("call_and_meeting_type", "")
            if meeting_type in allowed_types:
                meeting_counts[owner_id] += 1

        offset = data.get("offset")
        if not data.get("hasMore") or not offset:
            break

        page_count += 1

# Step 6: Write credentials to a temporary file
with open("service_account.json", "w") as f:
    f.write(credentials_json)

# Step 7: Authenticate with Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Step 8: Build enriched summaries
def sort_key(oid):
    email, first, last = owner_lookup.get(oid, ("", "", ""))
    return (last or "", first or "", oid)

call_rows = [["OwnerID", "Email", "FirstName", "LastName", "NumberOfCalls"]]
meeting_rows = [["OwnerID", "Email", "FirstName", "LastName", "NumberOfMeetings"]]

for owner_id in sorted(sales_owner_ids, key=sort_key):
    email, first, last = owner_lookup.get(owner_id, ("", "", ""))
    call_count = call_counts.get(owner_id, 0)
    meeting_count = meeting_counts.get(owner_id, 0)
    call_rows.append([owner_id, email, first, last, call_count])
    meeting_rows.append([owner_id, email, first, last, meeting_count])

# Step 9a: Overwrite "number_of_calls_august" sheet
spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1HkvNSwUatcwilCFjUGktQfqRtZE_BHsKbRt_JnU_K7Y/edit"
)

try:
    call_sheet = spreadsheet.worksheet("number_of_calls_august")
except gspread.exceptions.WorksheetNotFound:
    call_sheet = spreadsheet.add_worksheet(title="number_of_calls_august", rows="1000", cols="10")

call_sheet.clear()
call_sheet.update(values=call_rows, range_name="A1", value_input_option="RAW")

# Step 9b: Overwrite "meetings_august" sheet
try:
    meetings_sheet = spreadsheet.worksheet("meetings_august")
except gspread.exceptions.WorksheetNotFound:
    meetings_sheet = spreadsheet.add_worksheet(title="meetings_august", rows="1000", cols="10")

meetings_sheet.clear()
meetings_sheet.update(values=meeting_rows, range_name="A1", value_input_option="RAW")

print(f"✅ Overwrote 'number_of_calls_august' with {len(call_rows)-1} owners.")
print(f"✅ Overwrote 'meetings_august' with {len(meeting_rows)-1} owners.")

# Step 10: Clean up credentials file
os.remove("service_account.json")
