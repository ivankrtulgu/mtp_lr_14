import polars as pl
import duckdb
import time
import os
import sys

# Fix for Windows console encoding issues
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def benchmark_polars(file_path):
    start_time = time.perf_counter()
    
    # Read and perform aggregation exactly as in Task 6
    df = pl.read_parquet(file_path)
    result = (
        df.filter(pl.col("amount") > 0)
        .group_by("category")
        .agg([
            pl.col("amount").sum().alias("total_amount"),
            pl.col("amount").mean().alias("avg_amount"),
            pl.col("amount").min().alias("min_amount"),
            pl.col("amount").max().alias("max_amount"),
            pl.len().alias("transaction_count")
        ])
        .sort("total_amount", descending=True)
    )
    
    end_time = time.perf_counter()
    return result, (end_time - start_time) * 1000

def benchmark_duckdb(file_path):
    start_time = time.perf_counter()
    
    # Connect to in-memory DuckDB
    conn = duckdb.connect(database=':memory:')
    
    # Execute SQL query directly on the Parquet file
    # DuckDB can query Parquet files without importing them into a table first
    query = f"""
        SELECT 
            category, 
            SUM(amount) as total_amount, 
            AVG(amount) as avg_amount, 
            MIN(amount) as min_amount, 
            MAX(amount) as max_amount, 
            COUNT(*) as transaction_count
        FROM '{file_path}'
        WHERE amount > 0
        GROUP BY category
        ORDER BY total_amount DESC
    """
    result = conn.execute(query).pl() # Export result as Polars DataFrame for easy printing
    conn.close()
    
    end_time = time.perf_counter()
    return result, (end_time - start_time) * 1000

if __name__ == "__main__":
    parquet_file = "data/processed/transactions.parquet"
    
    if not os.path.exists(parquet_file):
        print(f"Error: {parquet_file} not found. Please run the ETL pipeline first.")
        sys.exit(1)
        
    print("--- Task 8: DuckDB SQL Analysis vs Polars Benchmark ---")
    
    # Polars Benchmark
    pl_res, pl_time = benchmark_polars(parquet_file)
    print("\n[Polars Result]:")
    print(pl_res)
    print(f"Execution time: {pl_time:.3f} ms")
    
    # DuckDB Benchmark
    db_res, db_time = benchmark_duckdb(parquet_file)
    print("\n[DuckDB Result]:")
    print(db_res)
    print(f"Execution time: {db_time:.3f} ms")
    
    # Final Comparison
    print("\n" + "="*40)
    print(f"PERFORMANCE COMPARISON:")
    print(f"Polars: {pl_time:.3f} ms")
    print(f"DuckDB: {db_time:.3f} ms")
    
    diff = abs(pl_time - db_time)
    winner = "DuckDB" if db_time < pl_time else "Polars"
    print(f"Winner: {winner} (faster by {diff:.3f} ms)")
    print("="*40)
