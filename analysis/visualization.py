import polars as pl
import matplotlib.pyplot as plt
import plotly.express as px
import os
import sys

# Fix for Windows console encoding issues
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def create_visualizations():
    # Path to the processed Parquet file
    input_path = "data/processed/transactions.parquet"
    plots_dir = "plots"
    
    if not os.path.exists(input_path):
        print(f"Error: File {input_path} not found. Please run the ETL pipeline first.")
        return

    if not os.path.exists(plots_dir):
        os.makedirs(plots_dir)

    # Load cleaned data
    df = pl.read_parquet(input_path)
    print("--- Task 9: Data Visualization ---")

    # --- Visualization 1: Spending by Category (Pie Chart) ---
    # Aggregate data for the pie chart
    category_data = (
        df.group_by("category")
        .agg(pl.col("amount").sum())
        .sort("amount", descending=True)
    ).to_pandas() # Plotly works best with pandas for simple charts

    fig_pie = px.pie(
        category_data, 
        values='amount', 
        names='category', 
        title='Total Spending Distribution by Category',
        hole=0.3
    )
    
    pie_output = os.path.join(plots_dir, "category_distribution.html")
    fig_pie.write_html(pie_output)
    print(f"1. Saved Pie Chart to {pie_output}")

    # --- Visualization 2: Spending Trend over Time (Time Series) ---
    # Aggregate daily spending
    # Convert timestamp to date for daily grouping
    time_series_df = (
        df.with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.col("amount").sum().alias("daily_sum"))
        .sort("date")
    ).to_pandas()

    plt.figure(figsize=(12, 6))
    plt.plot(time_series_df['date'], time_series_df['daily_sum'], color='blue', linewidth=2, marker='o', markersize=4)
    plt.title('Daily Transaction Volume Over Time', fontsize=14)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Amount Spent', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    ts_output = os.path.join(plots_dir, "spending_trend.png")
    plt.savefig(ts_output)
    plt.close()
    print(f"2. Saved Time Series Plot to {ts_output}")

if __name__ == "__main__":
    create_visualizations()
