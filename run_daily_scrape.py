import pandas as pd
import os
import random
from datetime import date, timedelta

# --- Configuration ---
SCRAPING_DATA_FOLDER = 'data/scraping_data'
SITE_NAME = 'mcmaster_clone'

PRODUCTS_CSV = os.path.join(SCRAPING_DATA_FOLDER, 'products.csv')
SNAPSHOTS_CSV = os.path.join(SCRAPING_DATA_FOLDER, 'price_inventory_snapshots.csv')

# Ensure the scraping data folder exists
os.makedirs(SCRAPING_DATA_FOLDER, exist_ok=True)

# --- Simulated Raw Data from a Scraper ---
# This is our "source of truth". In reality, this would be live scraped data.
# We've included a dimensionally-equivalent steel and titanium screw.
SIMULATED_PRODUCTS = [
    {
        "sku": "91251A541", "fastener_type": "machine screw", "material": "stainless steel 316",
        "grade_or_alloy": "18-8", "diameter_mm": 5, "length_mm": 20, "price_per_unit": 0.55,
        "inventory": 1500, "manufacturer": "Generic Steel Co."
    },
    {
        "sku": "91251A542", "fastener_type": "machine screw", "material": "alloy steel",
        "grade_or_alloy": "Grade 8", "diameter_mm": 5, "length_mm": 20, "price_per_unit": 0.45,
        "inventory": 2500, "manufacturer": "Generic Steel Co."
    },
    {
        "sku": "T91251A541", "fastener_type": "machine screw", "material": "titanium",
        "grade_or_alloy": "Ti-6Al-4V", "diameter_mm": 5, "length_mm": 20, "price_per_unit": 1.25,
        "inventory": 400, "manufacturer": "Premium Ti Supply"
    },
    {
        "sku": "HWB200", "fastener_type": "hex bolt", "material": "alloy steel",
        "grade_or_alloy": "Grade 5", "diameter_mm": 10, "length_mm": 50, "price_per_unit": 1.10,
        "inventory": 800, "manufacturer": "Bolts'R'Us"
    },
    {
        "sku": "HWB201_TI", "fastener_type": "hex bolt", "material": "titanium",
        "grade_or_alloy": "Grade 2", "diameter_mm": 10, "length_mm": 50, "price_per_unit": 3.50,
        "inventory": 200, "manufacturer": "Premium Ti Supply"
    }
]

def get_current_date(snapshots_df):
    """Gets the date for the current run. Increments day if data already exists."""
    if snapshots_df.empty:
        return date.today()
    else:
        last_date = pd.to_datetime(snapshots_df['date_scraped']).max().date()
        return last_date + timedelta(days=1)

def simulate_daily_changes(products_df, snapshots_df):
    """Generates a new list of product data with simulated daily changes."""
    today = get_current_date(snapshots_df)
    print(f"Simulating scrape for date: {today.strftime('%Y-%m-%d')}")

    output_data = []
    for product_data in SIMULATED_PRODUCTS:
        product_id = f"{SITE_NAME}_{product_data['sku']}"
        
        # Get the most recent snapshot for this product to simulate changes
        last_snapshot = snapshots_df[snapshots_df['product_id'] == product_id]
        if not last_snapshot.empty:
            last_inventory = last_snapshot.iloc[-1]['inventory_level']
            last_price = last_snapshot.iloc[-1]['price_per_unit']
        else:
            last_inventory = product_data['inventory']
            last_price = product_data['price_per_unit']

        # Simulate inventory depletion
        depletion = random.randint(int(last_inventory * 0.01), int(last_inventory * 0.05))
        new_inventory = last_inventory - depletion
        
        # Simulate occasional restocking
        if random.random() < 0.05: # 5% chance of restocking
            print(f"  -> Restocking {product_id}")
            new_inventory += product_data['inventory'] # Add original stock amount

        # Simulate minor price fluctuations
        price_change = random.uniform(-0.03, 0.03)
        new_price = round(last_price * (1 + price_change), 2)

        # Create a copy and update with dynamic data
        new_data = product_data.copy()
        new_data['price_per_unit'] = new_price
        new_data['inventory'] = max(0, new_inventory) # Ensure inventory doesn't go below 0
        new_data['date_scraped'] = today
        output_data.append(new_data)
        
    return output_data

def main():
    """Main function to run the ETL process."""
    # 1. Load existing data or create empty DataFrames
    if os.path.exists(PRODUCTS_CSV):
        products_df = pd.read_csv(PRODUCTS_CSV)
    else:
        products_df = pd.DataFrame(columns=['product_id', 'site', 'sku', 'fastener_type', 'material', 'grade_or_alloy', 'diameter_mm', 'length_mm', 'manufacturer', 'first_seen_date', 'last_seen_date'])

    if os.path.exists(SNAPSHOTS_CSV):
        snapshots_df = pd.read_csv(SNAPSHOTS_CSV)
    else:
        snapshots_df = pd.DataFrame(columns=['snapshot_id', 'product_id', 'date_scraped', 'price_per_unit', 'inventory_level'])

    # 2. Get today's "scraped" data
    scraped_data = simulate_daily_changes(products_df, snapshots_df)
    today_str = scraped_data[0]['date_scraped'].strftime('%Y-%m-%d')
    
    new_products = []
    new_snapshots = []

    # 3. Process each "scraped" item
    for item in scraped_data:
        product_id = f"{SITE_NAME}_{item['sku']}"

        # --- Update Products Table ---
        if product_id not in products_df['product_id'].values:
            print(f"  -> New product found: {product_id}. Adding to products table.")
            new_product = {
                'product_id': product_id,
                'site': SITE_NAME,
                'sku': item['sku'],
                'fastener_type': item['fastener_type'],
                'material': item['material'],
                'grade_or_alloy': item['grade_or_alloy'],
                'diameter_mm': item['diameter_mm'],
                'length_mm': item['length_mm'],
                'manufacturer': item['manufacturer'],
                'first_seen_date': today_str,
                'last_seen_date': today_str
            }
            new_products.append(new_product)
        else:
            # Update the 'last_seen_date' for existing products
            products_df.loc[products_df['product_id'] == product_id, 'last_seen_date'] = today_str

        # --- Always create a new snapshot ---
        new_snapshot = {
            'product_id': product_id,
            'date_scraped': today_str,
            'price_per_unit': item['price_per_unit'],
            'inventory_level': item['inventory']
        }
        new_snapshots.append(new_snapshot)
    
    # 4. Append new data and save
    if new_products:
        products_df = pd.concat([products_df, pd.DataFrame(new_products)], ignore_index=True)
    
    if new_snapshots:
        new_snapshots_df = pd.DataFrame(new_snapshots)
        # Add snapshot_id
        last_id = snapshots_df['snapshot_id'].max() if not snapshots_df.empty else -1
        new_snapshots_df['snapshot_id'] = range(last_id + 1, last_id + 1 + len(new_snapshots_df))
        snapshots_df = pd.concat([snapshots_df, new_snapshots_df], ignore_index=True)

    products_df.to_csv(PRODUCTS_CSV, index=False)
    snapshots_df.to_csv(SNAPSHOTS_CSV, index=False)
    
    print("\nDaily scrape simulation complete. CSV files have been updated.")
    print(f"Total products: {len(products_df)}")
    print(f"Total snapshots: {len(snapshots_df)}")

if __name__ == "__main__":
    main()