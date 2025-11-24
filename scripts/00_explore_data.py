import pandas as pd
import numpy as np

df = pd.read_csv('../data/raw/ecommerce.csv', encoding='ISO-8859-1')

print("\n" + "="*50)
print("DATASET INFORMATION")
print("="*50)
print(f"Total Rows: {len(df)}")
print(f"Total Columns: {len(df.columns)}")
print(f"\nColumns: {list(df.columns)}")

print("\n" + "="*50)
print("MISSING VALUES")
print("="*50)
print(df.isnull().sum())

print("\n" + "="*50)
print("SAMPLE DATA (First 5 Rows)")
print("="*50)
print(df.head())

print("\n" + "="*50)
print("BASIC STATISTICS")
print("="*50)
print(df.describe())

if 'Country' in df.columns:
    print(f"\nTotal Unique Countries: {df['Country'].nunique()}")
    print(f"Top 10 Countries by Transaction:")
    print(df['Country'].value_counts().head(10))

if 'Description' in df.columns:
    print(f"\nTotal Unique Products: {df['Description'].nunique()}")

if 'InvoiceDate' in df.columns:
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    print(f"\nDate Range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")

print("\n" + "="*50)
print("EXPLORATION COMPLETE!")
print("="*50)