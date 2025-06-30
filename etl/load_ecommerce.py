import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent
PATH = BASE_DIR / "data" / "online_retail.csv"
if not PATH.exists():
    raise FileNotFoundError(f"Data file not found at {PATH}. Please check the path.")
DB_URL = os.environ.get("DB_URL")

if not DB_URL:
    raise ValueError("DB_URL not found in environment variables. Check your .env file.")

engine = create_engine(DB_URL)

df = pd.read_csv(PATH, encoding="ISO-8859-1")

# 1. Clean
# Filter out cancelled invoices (InvoiceNo starts with 'C')
df = df[~df['InvoiceNo'].str.startswith('C', na=False)]
# Ignore negative Quantity rows and drop rows with null CustomerID
df = df[df['Quantity'] > 0]
df.dropna(subset=['CustomerID'], inplace=True)
df['CustomerID'] = df['CustomerID'].astype(int)

# 2. Transform
# Parse InvoiceDate as M/D/YYYY H:MM
df['InvoiceDate'] = pd.to_datetime(
    df['InvoiceDate'],
    format='%m/%d/%Y %H:%M',   # month–day–year
    dayfirst=False,            # keep month first
    errors='raise'             # stop if something doesn't match
)
# Create revenue_gbp
df['Revenue'] = df['Quantity'] * df['UnitPrice']

# 3. Load dim tables -------------------------------------------------
print("Loading dim_date...")
date_df = (df[['InvoiceDate']]
           .drop_duplicates() # Process unique InvoiceDate values
           .assign(
               date_key=lambda x: x['InvoiceDate'].dt.date,
               day=lambda x: x['InvoiceDate'].dt.day,
               week=lambda x: x['InvoiceDate'].dt.isocalendar().week.astype(int),
               month=lambda x: x['InvoiceDate'].dt.month,
               quarter=lambda x: x['InvoiceDate'].dt.quarter,
               year=lambda x: x['InvoiceDate'].dt.year
           )
           .drop(columns=['InvoiceDate']) # Remove the original InvoiceDate column
           .drop_duplicates(subset=['date_key']) # Ensure date_key is unique for the PK
           .set_index('date_key'))

# Use raw SQL to handle duplicates
with engine.connect() as conn:
    for _, row in date_df.iterrows():
        conn.execute(text("""
            INSERT INTO dim_date (date_key, day, week, month, quarter, year) 
            VALUES (:date_key, :day, :week, :month, :quarter, :year) 
            ON CONFLICT (date_key) DO NOTHING
        """), {
            'date_key': row.name,
            'day': int(row['day']),
            'week': int(row['week']),
            'month': int(row['month']),
            'quarter': int(row['quarter']),
            'year': int(row['year'])
        })
    conn.commit()
print("dim_date loaded.")

print("Loading dim_customer...")
customer_df = (
    df[['CustomerID', 'Country']]
    .drop_duplicates(subset=['CustomerID'])
    .rename(columns={
        'CustomerID': 'customer_id',
        'Country': 'country'          
    })
)

# Handle duplicates for customers
with engine.connect() as conn:
    for _, row in customer_df.iterrows():
        conn.execute(text("""
            INSERT INTO dim_customer (customer_id, country) 
            VALUES (:customer_id, :country) 
            ON CONFLICT (customer_id) DO NOTHING
        """), {
            'customer_id': int(row['customer_id']),
            'country': row['country']
        })
    conn.commit()
print("dim_customer loaded.")

print("Loading dim_product...")
product_df = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'])
# Handle cases where Description might be missing or inconsistent for the same StockCode
product_df = product_df.dropna(subset=['Description'])
product_df.rename(columns={'StockCode': 'stock_code', 'Description': 'description'}, inplace=True)

# Handle duplicates for products
with engine.connect() as conn:
    for _, row in product_df.iterrows():
        conn.execute(text("""
            INSERT INTO dim_product (stock_code, description) 
            VALUES (:stock_code, :description) 
            ON CONFLICT (stock_code) DO NOTHING
        """), {
            'stock_code': row['stock_code'],
            'description': row['description']
        })
    conn.commit()
print("dim_product loaded.")

print("Loading dim_invoice...")
invoice_df = df[['InvoiceNo']].drop_duplicates(subset=['InvoiceNo'])
invoice_df['is_cancelled'] = False # Already filtered out cancellations
invoice_df.rename(columns={'InvoiceNo': 'invoice_no'}, inplace=True)

# Handle duplicates for invoices
with engine.connect() as conn:
    for _, row in invoice_df.iterrows():
        conn.execute(text("""
            INSERT INTO dim_invoice (invoice_no, is_cancelled) 
            VALUES (:invoice_no, :is_cancelled) 
            ON CONFLICT (invoice_no) DO NOTHING
        """), {
            'invoice_no': row['invoice_no'],
            'is_cancelled': bool(row['is_cancelled'])
        })
    conn.commit()
print("dim_invoice loaded.")

# 4. Load fact table --------------------------------------------------
print("Preparing fct_sales...")
# Retrieve surrogate keys from dimension tables
dim_customer_sk = pd.read_sql_table('dim_customer', engine, columns=['customer_key', 'customer_id'])
dim_product_sk = pd.read_sql_table('dim_product', engine, columns=['product_key', 'stock_code'])
dim_invoice_sk = pd.read_sql_table('dim_invoice', engine, columns=['invoice_key', 'invoice_no'])
# dim_date_sk is implicitly handled by using InvoiceDate.dt.date as date_key

# Prepare fact_df by merging with dimension surrogate keys
fact_df = df.copy()
fact_df['date_key'] = fact_df['InvoiceDate'].dt.date

fact_df = pd.merge(fact_df, dim_customer_sk, left_on='CustomerID', right_on='customer_id', how='left')
fact_df = pd.merge(fact_df, dim_product_sk, left_on='StockCode', right_on='stock_code', how='left')
fact_df = pd.merge(fact_df, dim_invoice_sk, left_on='InvoiceNo', right_on='invoice_no', how='left')

# Select and rename columns for fct_sales
fct_sales_df = fact_df[[
    'invoice_key', 
    'product_key', 
    'customer_key', 
    'date_key', 
    'Quantity', 
    'UnitPrice', 
    'Revenue'
]]

fct_sales_df = fct_sales_df.rename(columns={
    'Quantity': 'quantity', 
    'UnitPrice': 'unit_price', 
    'Revenue': 'revenue_gbp'
})

# Drop rows where any foreign key is NaN (due to missing dim record, though should be rare with append)
fct_sales_df.dropna(subset=['invoice_key', 'product_key', 'customer_key', 'date_key'], inplace=True)

# Ensure correct dtypes for keys before loading (e.g. int)
fct_sales_df['invoice_key'] = fct_sales_df['invoice_key'].astype(int)
fct_sales_df['product_key'] = fct_sales_df['product_key'].astype(int)
fct_sales_df['customer_key'] = fct_sales_df['customer_key'].astype(int)


print("Loading fct_sales...")
fct_sales_df.to_sql('fct_sales', engine, if_exists='append', index=False, chunksize=10000)
print("fct_sales loaded.")
print("ETL process completed successfully.")