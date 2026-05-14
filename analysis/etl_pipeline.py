import polars as pl
import os
import sys

# Fix for Windows console encoding issues
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def import_data():
    input_path = "data/intermediate/transactions.jsonl"
    if not os.path.exists(input_path):
        print(f"Error: File {input_path} not found.")
        return None
    return pl.read_ndjson(input_path)

def clean_data(df):
    print("\n--- Task 5: Data Cleaning and Validation ---")
    initial_count = df.shape[0]
    
    # Step 1: Remove duplicates based on transaction_id
    # We keep the first occurrence and remove subsequent duplicates
    df = df.unique(subset=["transaction_id"])
    after_dup_count = df.shape[0]
    print(f"1. Removed duplicates: {initial_count - after_dup_count} rows removed.")

    # Step 2: Handle missing values
    # A. Fill empty/null categories with 'Other'
    df = df.with_columns(
        pl.when(pl.col("category").is_null() | (pl.col("category") == ""))
        .then(pl.lit("Other"))
        .otherwise(pl.col("category"))
        .alias("category")
    )
    
    # B. Handle missing amounts
    # In our Go collector, empty amounts became 0.0. We treat 0.0 as missing for this lab.
    median_amount = df.filter(pl.col("amount") != 0).select(pl.col("amount").median()).item()
    df = df.with_columns(
        pl.when(pl.col("amount") == 0)
        .then(pl.lit(median_amount))
        .otherwise(pl.col("amount"))
        .alias("amount")
    )
    print(f"2. Handled missing values: Categories filled with 'Other', Amounts filled with median ({median_amount:.2f}).")

    # Step 3: Type casting
    # Convert timestamp string to Datetime
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime()
    )
    print("3. Type casting: 'timestamp' converted to Datetime.")

    print(f"\nCleaning complete. Final row count: {df.shape[0]}")
    return df

if __name__ == "__main__":
    # Task 4: Import
    df_raw = import_data()
    if df_raw is not None:
        print("Data imported successfully.")
        
        # Task 5: Clean
        df_cleaned = clean_data(df_raw)
        
        print("\nCleaned Data Preview:")
        print(df_cleaned.head(5))
        print("\nCleaned Schema:")
        print(df_cleaned.schema)

