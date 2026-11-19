import requests
from pathlib import Path

# nascar 2025 cup series race list

url = "https://cf.nascar.com/cacher/2025/1/race_list_basic.json"

#sending a get request to the url
response = requests.get(url, timeout=30)
response.raise_for_status()  # errror for bad responses

#converting json to python list of records
races = response.json()

raw_dir = Path(__file__).resolve().parent / "data" / "raw"
raw_dir.mkdir(parents=True, exist_ok=True)

output_file = raw_dir / "race_list_2025.json"
output_file.write_bytes(response.content)
print("Saved race list to", output_file)


first_race = races[0]
print("Events returned: ", len(races))
print("first event :", first_race["race_name"])



#fetching one race details

# Select the 36 points races from the event list.
points_races = [event for event in races if event["race_type_id"] == 1]
saved_count = 0

for event in points_races:
    # Use this event's ID in the same URL pattern we tested with Daytona.
    race_id = event["race_id"]
    weekend_url = f"https://cf.nascar.com/cacher/2025/1/{race_id}/weekend-feed.json"
    response = requests.get(weekend_url, timeout=30)
    response.raise_for_status()

    # Confirm the response contains results for the race we requested.
    race = response.json()["weekend_race"][0]
    if race["race_id"] != race_id or not race["results"]:
        raise ValueError(f"Missing or mismatched results for race {race_id}")

    # Save the original JSON using the race ID and date.
    race_date = race["race_date"][:10]
    output_file = raw_dir / f"race_{race_id}_{race_date}.json"
    output_file.write_bytes(response.content)
    saved_count += 1
    print("Saved:", output_file)

print(f"Weekend files saved: {saved_count}/{len(points_races)}")
