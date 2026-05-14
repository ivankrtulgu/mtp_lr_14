import polars as pl
import os
import sys

# Fix for Windows console encoding issues
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def import_data():
    # Path to the intermediate JSONL file
    input_path = "data/intermediate/transactions.jsonl"
    
    if not os.path.exists(input_path):
        print(f"Error: File {input_path} not found. Please run the Go collector first.")
        return None

    print("--- Task 4: Importing Data into Polars ---")
    
    # 1. Load JSONL data into a Polars DataFrame
    # read_ndjson is the fastest way to load newline-delimited JSON
    df = pl.read_ndjson(input_path)
    
    # 2. Output the first 5 rows
    print("\nFirst 5 rows of the dataset:")
    print(df.head(5))
    
    # 3. Basic information about the data
    print("\n--- Basic Data Information ---")
    
    # Row count
    print(f"Total number of rows: {df.shape[0]}")
    
    # Data types (Schema)
    print("\nColumn Types:")
    print(df.schema)
    
    # Missing values (Null count)
    print("\nMissing Values (Null Count) per column:")
    print(df.null_count())
    
    return df

if __name__ == "__main__":
    # Execute the import process
    df_transactions = import_data()
