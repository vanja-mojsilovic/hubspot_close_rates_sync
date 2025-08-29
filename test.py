import json
with open("service_account.json") as f:
    print(json.dumps(json.load(f)))
