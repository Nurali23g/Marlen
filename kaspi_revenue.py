import pandas as pd
import os

FOLDER = "kaspi_sellers"   # folder with all .xlsx files
OUTPUT_FILE = "all_sellers_revenue.csv"

all_data = []

for file in os.listdir(FOLDER):
    if file.endswith(".xlsx"):
        filepath = os.path.join(FOLDER, file)
        print("Processing:", filepath)

        # Read Excel, skip first 4 rows
        df = pd.read_excel(filepath, skiprows=4)

        # Group by "Наименование магазина"
        grouped = df.groupby("Наименование магазина", as_index=False)["Общая выручка за период"].sum()
        grouped["Файл"] = file  

        all_data.append(grouped)

# Combine all
final_df = pd.concat(all_data, ignore_index=True)

# Group again across all files
final_df = final_df.groupby("Наименование магазина", as_index=False)["Общая выручка за период"].sum()

# Save result
final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print("Saved to", OUTPUT_FILE)
