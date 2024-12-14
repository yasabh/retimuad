import os
import pandas as pd

# Directories
input_folder = "./dataset"
output_folder = "./dataset/filtered"
os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist

# Columns to keep
columns_to_keep = ["Timestamp", "MV101", "LIT101", "Normal/Attack"]

# Date to filter (24-hour range for this date)
filter_date = "29/12/2015"  # Adjust as needed

# Function to filter a single file
def filter_file(file_path):
    print(f"Processing file: {file_path}")
    data = pd.read_csv(file_path, sep=",")  # Adjust `sep` if needed (e.g., "," for commas)

    # Inspect and clean column names
    data.columns = data.columns.str.strip()
    print("Columns in dataset:", data.columns.tolist())

    # Filter columns
    filtered_data = data.loc[:, columns_to_keep]  # Use `.loc[]` to explicitly select columns

    # Clean and convert Timestamp
    filtered_data["Timestamp"] = filtered_data["Timestamp"].str.strip()  # Remove extra spaces
    filtered_data["Timestamp"] = pd.to_datetime(
        filtered_data["Timestamp"], format="%d/%m/%Y %I:%M:%S %p", errors="coerce"
    )

    # Drop rows with invalid Timestamps
    filtered_data = filtered_data.dropna(subset=["Timestamp"])

    # Filter rows based on the date
    filtered_data = filtered_data.loc[filtered_data["Timestamp"].dt.strftime("%d/%m/%Y") == filter_date]

    # Check if the DataFrame is empty
    if filtered_data.empty:
        print(f"No data found for date {filter_date} in {file_path}")
        return None  # Return None if no data matches

    return filtered_data


# Loop through all CSV files in the input folder
print("Scanning for CSV files...")
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

merged_data = []

for csv_file in csv_files:
    file_path = os.path.join(input_folder, csv_file)

    # Filter the file
    filtered_data = filter_file(file_path)
    if filtered_data is not None:
        # Append to merged dataset
        merged_data.append(filtered_data)

# Merge all filtered data if any exists
if merged_data:
    print("Merging all filtered datasets...")
    merged_dataset = pd.concat(merged_data)
    merged_output_file = os.path.join(output_folder, "dataset.csv")
    merged_dataset.to_csv(merged_output_file, index=False)
    print(f"Merged dataset saved to {merged_output_file}")
else:
    print("No data to merge. No filtered datasets found.")
