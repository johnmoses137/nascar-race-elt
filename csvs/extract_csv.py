import requests
import pandas as pd
from pathlib import Path

# NASCAR 2025 Cup Series race events API
url = "https://cf.nascar.com/cacher/2025/1/race_list_basic.json"

# Fetch data from API
response = requests.get(url, timeout=30)
response.raise_for_status()

# Convert JSON response into Python records
races = response.json()

# Convert to Pandas DataFrame
df = pd.DataFrame(races)

# Save CSV in the same folder as this script
output_file = Path(__file__).resolve().parent / "race_events_2025.csv"

df.to_csv(output_file, index=False)

print("Total events:", len(df))
print("CSV saved to:", output_file)


# Select only points-paying races
points_races = [race for race in races if race["race_type_id"] == 1]

all_results = []

# Fetch weekend results for each race
for event in points_races:
    race_id = event["race_id"]

    weekend_url = f"https://cf.nascar.com/cacher/2025/1/{race_id}/weekend-feed.json"

    response = requests.get(weekend_url, timeout=30)
    response.raise_for_status()

    race = response.json()["weekend_race"][0]

    # Validate the race ID and results
    if race["race_id"] != race_id or not race["results"]:
        raise ValueError(f"Missing or mismatched results for race {race_id}")

    # Add race ID to each driver's record
    for driver in race["results"]:
        driver_record = driver.copy()
        driver_record["race_id"] = race_id
        all_results.append(driver_record)

    print(f"Fetched results for race {race_id}")

# Combine all driver results into one DataFrame
results_df = pd.DataFrame(all_results)

# Save all results to a single CSV
results_file = Path(__file__).resolve().parent / "weekend_results_2025.csv"

results_df.to_csv(results_file, index=False)

print("Total races processed:", len(points_races))
print("Total driver records:", len(results_df))
print("CSV saved to:", results_file)