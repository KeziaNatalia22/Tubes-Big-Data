import pandas as pd
import subprocess
import os

print("="*60)
print("DATA INGESTION TO HDFS")
print("="*60)

print("\n[1/4] Loading data from CSV...")
df = pd.read_csv('../data/raw/ecommerce.csv', encoding='ISO-8859-1')
print(f"Total rows loaded: {len(df)}")

print("\n[2/4] Cleaning data...")

initial_rows = len(df)
df = df.dropna(subset=['CustomerID'])
print(f"Removed {initial_rows - len(df)} rows with missing CustomerID")

df = df[df['Quantity'] > 0]

df = df[df['UnitPrice'] > 0]

df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

df['Year'] = df['InvoiceDate'].dt.year
df['Month'] = df['InvoiceDate'].dt.month
df['YearMonth'] = df['InvoiceDate'].dt.to_period('M').astype(str)
df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

df['ProductCategory'] = df['Description'].str.split().str[0]

print(f"Final rows after cleaning: {len(df)}")
print(f"Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")

print("\n[3/4] Saving cleaned data...")
cleaned_file = '../data/processed/ecommerce_cleaned.csv'
df.to_csv(cleaned_file, index=False)
print(f"Cleaned data saved to: {cleaned_file}")

print("\n[4/4] Uploading to HDFS...")

cmd_copy = f"docker cp {cleaned_file} namenode:/tmp/ecommerce_cleaned.csv"
subprocess.run(cmd_copy, shell=True, check=True)

cmd_upload = "docker exec namenode hdfs dfs -put -f /tmp/ecommerce_cleaned.csv /data/raw/"
subprocess.run(cmd_upload, shell=True, check=True)

print("\nVerifying upload...")
cmd_verify = "docker exec namenode hdfs dfs -ls /data/raw/"
subprocess.run(cmd_verify, shell=True, check=True)

cmd_info = "docker exec namenode hdfs dfs -du -h /data/raw/ecommerce_cleaned.csv"
subprocess.run(cmd_info, shell=True, check=True)

print(f"\nData location in HDFS: /data/raw/ecommerce_cleaned.csv")
print(f"Total rows ingested: {len(df)}")
print(f"File size on HDFS: Check output above")