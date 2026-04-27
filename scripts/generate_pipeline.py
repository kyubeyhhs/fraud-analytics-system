import pandas as pd
import numpy as np
import duckdb

# CONFIGURATION
# Simplified paths - assumes script is run from the project root
WAREHOUSE_PATH = "warehouse/fraud_analytics.duckdb"
RAW_DATA = "data/raw/transactions.csv"
IP_MAPPING = "data/raw/ip_to_country.csv"
PROCESSED_DIR = "data/processed/"

# DATA LOADING & INITIAL CLEANING
print("Loading raw datasets...")
df = pd.read_csv(RAW_DATA)
df_ip_map = pd.read_csv(IP_MAPPING)

# Immediate IP Sanitization: Fixes decimal/float issues at the source
df['ip_address'] = pd.to_numeric(df['ip_address'], errors='coerce').fillna(0).round(0).astype('int64')
df_ip_map['lower_bound_ip_address'] = df_ip_map['lower_bound_ip_address'].astype('int64')
df_ip_map['upper_bound_ip_address'] = df_ip_map['upper_bound_ip_address'].astype('int64')

# STANDARDIZATION
# Standardize column names and types
df = df.rename(columns={'class': 'is_fraud'})
df['purchase_value'] = df['purchase_value'].abs()
df['signup_time'] = pd.to_datetime(df['signup_time'])
df['purchase_time'] = pd.to_datetime(df['purchase_time'])

# Create a clean date-only column for the Calendar join in Power BI
df['event_date'] = df['purchase_time'].dt.date

# Calculate velocity and synthetic chargeback flag
df['velocity_seconds'] = (df['purchase_time'] - df['signup_time']).dt.total_seconds()
df['is_chargeback'] = np.random.binomial(1, np.where(df['is_fraud'] == 1, 0.8, 0.01))

# WAREHOUSE PROCESSING (DuckDB)
# Use DuckDB for the heavy inequality join
with duckdb.connect(WAREHOUSE_PATH) as db:
    db.register('stg_transactions', df)
    db.register('stg_ip_map', df_ip_map)

    # Perform inequality join and create the core Fact table
    f_transactions = db.execute("""
        SELECT t.*, COALESCE(i.country, 'Unknown') as ip_country
        FROM stg_transactions t
        LEFT JOIN stg_ip_map i ON t.ip_address >= i.lower_bound_ip_address 
                             AND t.ip_address <= i.upper_bound_ip_address
    """).df()

    # Save Fact table to warehouse
    db.execute("CREATE OR REPLACE TABLE f_transactions AS SELECT * FROM f_transactions")

    # Generate Dimension tables
    d_chargebacks = f_transactions[f_transactions['is_chargeback'] == 1].copy()
    d_chargebacks['cb_status'] = np.random.choice(['Won', 'Lost', 'Pending'], size=len(d_chargebacks))
    
    d_alerts = f_transactions.sample(frac=0.03).copy()
    d_alerts['priority'] = np.where(d_alerts['is_fraud'] == 1, 'High', 'Medium')

    # Save Dimensions to warehouse
    db.register('stg_cb', d_chargebacks)
    db.execute("CREATE OR REPLACE TABLE d_chargebacks AS SELECT * FROM stg_cb")
    
    db.register('stg_alerts', d_alerts)
    db.execute("CREATE OR REPLACE TABLE d_alerts AS SELECT * FROM stg_alerts")

# --- 5. EXPORTS FOR POWER BI ---
# Exporting with prefixes to match the Power BI Star Schema model
f_transactions.to_csv(f"{PROCESSED_DIR}f_transactions.csv", index=False)
d_chargebacks.to_csv(f"{PROCESSED_DIR}d_chargebacks.csv", index=False)
d_alerts.to_csv(f"{PROCESSED_DIR}d_alerts.csv", index=False)

print("Pipeline complete. All tables synchronized for Power BI.")