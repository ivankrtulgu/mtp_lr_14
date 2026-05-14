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
    df = df.unique(subset=["transaction_id"])
    after_dup_count = df.shape[0]
    print(f"1. Removed duplicates: {initial_count - after_dup_count} rows removed.")

    # Step 2: Handle missing values
    df = df.with_columns(
        pl.when(pl.col("category").is_null() | (pl.col("category") == ""))
        .then(pl.lit("Other"))
        .otherwise(pl.col("category"))
        .alias("category")
    )
    
    median_amount = df.filter(pl.col("amount") != 0).select(pl.col("amount").median()).item()
    df = df.with_columns(
        pl.when(pl.col("amount") == 0)
        .then(pl.lit(median_amount))
        .otherwise(pl.col("amount"))
        .alias("amount")
    )
    print(f"2. Handled missing values: Categories filled with 'Other', Amounts filled with median ({median_amount:.2f}).")

    # Step 3: Type casting
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime()
    )
    print("3. Type casting: 'timestamp' converted to Datetime.")

    print(f"\nCleaning complete. Final row count: {df.shape[0]}")
    return df

def perform_aggregation(df):
    print("\n--- Task 6: Aggregation Analysis ---")
    
    # Group by category and calculate SUM, AVG, MIN, MAX, COUNT
    summary = (
        df.group_by("category")
        .agg([
            pl.col("amount").sum().alias("total_amount"),
            pl.col("amount").mean().alias("avg_amount"),
            pl.col("amount").min().alias("min_amount"),
            pl.col("amount").max().alias("max_amount"),
            pl.len().alias("transaction_count")
        ])
        .sort("total_amount", descending=True)
    )
    
    print("\nSummary by Category:")
    print(summary)
    return summary

if __name__ == "__main__":
    # Task 4: Import
    df_raw = import_data()
    if df_raw is not None:
        print("Data imported successfully.")
        
        # Task 5: Clean
        df_cleaned = clean_data(df_raw)
        
        # Task 6: Aggregate
        df_summary = perform_aggregation(df_cleaned)


