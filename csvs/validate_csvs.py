import pandas as pd
from pathlib import Path

csv_dir = Path(__file__).resolve().parent

# Read existing CSV files
events_df = pd.read_csv(csv_dir / "race_events_2025.csv")
results_df = pd.read_csv(csv_dir / "weekend_results_2025.csv")

# Validate the data
print("Total events:", len(events_df))
print("Total races with results:", results_df["race_id"].nunique())
print("Total driver records:", len(results_df))

# Check for duplicate driver results within the same race
duplicates = results_df.duplicated(
    subset=["race_id", "driver_id"]
).sum()

print("Duplicate driver records:", duplicates)

print("First few rows of results:")
print(results_df.head())


print("First few rows of events:")
print(events_df.head())