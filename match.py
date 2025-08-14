import os
import pandas as pd

# Folder containing CSV files
folder_path = r"d:/Profiles/Desktop/ijohnson/Desktop/HubSpot/hubspot_data"

# Get list of CSV files
csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]

relationships = []

for file1 in csv_files:
    path1 = os.path.join(folder_path, file1)

    try:
        df1 = pd.read_csv(path1, dtype=str)
    except Exception as e:
        print(f"Skipping {file1} due to read error: {e}")
        continue

    # Find 'id' column (case-insensitive)
    id_col = [col for col in df1.columns if col.lower() == 'id']
    if not id_col:
        continue

    # Drop NaN and make set of unique IDs
    ids = set(df1[id_col[0]].dropna().astype(str))

    for file2 in csv_files:
        if file1 == file2:
            continue

        path2 = os.path.join(folder_path, file2)

        try:
            df2 = pd.read_csv(path2, dtype=str)
        except Exception as e:
            print(f"Skipping {file2} due to read error: {e}")
            continue

        # Compare 'id' values from file1 against all columns in file2
        for col in df2.columns:
            col_values = set(df2[col].dropna().astype(str))
            match_count = len(ids & col_values)
            if match_count > 0:
                relationships.append({
                    "File 1": file1,
                    "File 1 ID Column": id_col[0],
                    "File 2": file2,
                    "File 2 Column": col,
                    "Match Count": match_count
                })

# Output results
if relationships:
    result_df = pd.DataFrame(relationships)
    output_path = os.path.join(folder_path, "relationships_found.csv")
    result_df.to_csv(output_path, index=False)
    print(f"Relationships found! Saved to {output_path}")
else:
    print("No relationships found.")
