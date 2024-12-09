import os
import pandas as pd

# Directories
input_folder = "./dataset"
output_folder = "./dataset/filtered"
os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist

# Columns to keep
columns_to_keep = ["Timestamp", "LIT101", "LIT301", "LIT401", "FIT101", "Normal/Attack"]
# columns_to_keep = ["Timestamp", "MV304", "MV101","LIT101", "MV303", "LIT301", "AIT504","Normal/Attack"]

# Function to process a single file
def process_file(file_path):
    print(f"Processing file: {file_path}")
    data = pd.read_csv(file_path, sep=",")  # Adjust `sep` if needed (e.g., "," for commas)

    # Inspect and clean column names
    data.columns = data.columns.str.strip()
    print("Columns in dataset:", data.columns.tolist())

    # Filter columns
    processed_data = data.loc[:, columns_to_keep]  # Use `.loc[]` to explicitly select columns

    # Clean and convert Timestamp
    processed_data["Timestamp"] = processed_data["Timestamp"].str.strip()  # Remove extra spaces
    processed_data["Timestamp"] = pd.to_datetime(
        processed_data["Timestamp"], format="%d/%m/%Y %I:%M:%S %p", errors="coerce"
    )

    # Drop rows with invalid Timestamps
    processed_data = processed_data.dropna(subset=["Timestamp"])

    # Check if the DataFrame is empty
    if processed_data.empty:
        print(f"No valid timestamps found in {file_path}")
        return None  # Return None if no valid data

    return processed_data


# Loop through all CSV files in the input folder
print("Scanning for CSV files...")
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

merged_data = []

for csv_file in csv_files:
    file_path = os.path.join(input_folder, csv_file)

    # Process the file
    processed_data = process_file(file_path)
    if processed_data is not None:
        # Append to merged dataset
        merged_data.append(processed_data)

# Merge all processed data if any exists
if merged_data:
    print("Merging all processed datasets...")
    merged_dataset = pd.concat(merged_data)
    merged_output_file = os.path.join(output_folder, "dataset.csv")
    merged_dataset.to_csv(merged_output_file, index=False)
    print(f"Merged dataset saved to {merged_output_file}")
else:
    print("No data to merge. No processed datasets found.")
