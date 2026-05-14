import csv
import random
from datetime import datetime, timedelta

def generate_bank_transactions(filename="transactions.csv", num_rows=10000):
    categories = ["Food", "Transport", "Salary", "Utilities", "Entertainment", "Healthcare", "Shopping"]
    statuses = ["completed", "completed", "completed", "pending", "failed"]
    merchants = ["Amazon", "Walmart", "Starbucks", "Shell", "Netflix", "Apple", "Uber", "Local Grocery"]
    
    start_date = datetime(2025, 1, 1)
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Header
        writer.writerow(["transaction_id", "timestamp", "account_id", "amount", "category", "merchant", "status"])
        
        for i in range(num_rows):
            # Intentionally create some duplicates for Task 5
            # Every 100th row will be a duplicate of the previous row
            if i > 0 and i % 100 == 0:
                # We can't easily access the previous row in this loop without storing it, 
                # so we'll just manually repeat an ID occasionally.
                t_id = f"TXN_{i-1}"
            else:
                t_id = f"TXN_{i}"
            
            # Random date within 5 months
            days_offset = random.randint(0, 150)
            timestamp = (start_date + timedelta(days=days_offset)).strftime("%Y-%m-%d %H:%M:%S")
            
            account_id = f"ACC_{random.randint(100, 120)}"
            
            # Intentionally create some missing values (NaNs) for Task 5
            if random.random() < 0.02: # 2% chance of missing amount
                amount = ""
            else:
                amount = round(random.uniform(-500.0, 5000.0), 2)
                
            if random.random() < 0.02: # 2% chance of missing category
                category = ""
            else:
                category = random.choice(categories)
                
            merchant = random.choice(merchants)
            status = random.choice(statuses)
            
            writer.writerow([t_id, timestamp, account_id, amount, category, merchant, status])

    print(f"Successfully generated {num_rows} transactions in {filename}")

if __name__ == "__main__":
    generate_bank_transactions()
