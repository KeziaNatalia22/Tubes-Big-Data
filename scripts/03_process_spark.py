from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min
from pyspark.sql.functions import year, month, dayofmonth, to_date
import os

print("="*70)
print("SPARK SQL DATA PROCESSING")
print("="*70)

# Step 1: Create Spark Session
print("\n[1/7] Creating Spark Session...")
spark = SparkSession.builder \
    .appName("E-Commerce Analytics") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("Spark Session created successfully!")
print(f"Spark Version: {spark.version}")

# Step 2: Read data from HDFS
print("\n[2/7] Reading data from HDFS...")
df = spark.read.csv(
    "hdfs://namenode:9000/data/raw/ecommerce_cleaned.csv",
    header=True,
    inferSchema=True
)

print(f"Total rows: {df.count()}")
print(f"Total columns: {len(df.columns)}")

# Show schema
print("\nSchema:")
df.printSchema()

# Show sample data
print("\nSample data:")
df.show(5)

# Step 3: Register as temporary view for SQL queries
print("\n[3/7] Registering temporary view...")
df.createOrReplaceTempView("ecommerce")
print("View 'ecommerce' registered successfully!")

# Step 4: Query 1 - Sales per Month
print("\n[4/7] Query 1: Sales per Month...")
sales_per_month = spark.sql("""
    SELECT 
        YearMonth,
        COUNT(DISTINCT InvoiceNo) as TotalOrders,
        SUM(Quantity) as TotalQuantity,
        SUM(TotalPrice) as TotalRevenue,
        AVG(TotalPrice) as AvgOrderValue,
        COUNT(DISTINCT CustomerID) as UniqueCustomers
    FROM ecommerce
    GROUP BY YearMonth
    ORDER BY YearMonth
""")

print("Sales per Month:")
sales_per_month.show()

# Save to Parquet
print("Saving to Parquet...")
sales_per_month.write.mode("overwrite").parquet("hdfs://namenode:9000/output/sales_per_month")
print("✓ Saved to: /output/sales_per_month")

# Step 5: Query 2 - Sales per Category
print("\n[5/7] Query 2: Top 20 Product Categories...")
sales_per_category = spark.sql("""
    SELECT 
        ProductCategory,
        COUNT(DISTINCT InvoiceNo) as TotalOrders,
        SUM(Quantity) as TotalQuantitySold,
        SUM(TotalPrice) as TotalRevenue,
        AVG(UnitPrice) as AvgPrice,
        COUNT(DISTINCT CustomerID) as UniqueCustomers
    FROM ecommerce
    GROUP BY ProductCategory
    ORDER BY TotalRevenue DESC
    LIMIT 20
""")

print("Top 20 Product Categories:")
sales_per_category.show()

# Save to Parquet
print("Saving to Parquet...")
sales_per_category.write.mode("overwrite").parquet("hdfs://namenode:9000/output/sales_per_category")
print("✓ Saved to: /output/sales_per_category")

# Step 6: Query 3 - Sales per Country
print("\n[6/7] Query 3: Top 15 Countries by Revenue...")
sales_per_country = spark.sql("""
    SELECT 
        Country,
        COUNT(DISTINCT InvoiceNo) as TotalOrders,
        SUM(Quantity) as TotalQuantity,
        SUM(TotalPrice) as TotalRevenue,
        AVG(TotalPrice) as AvgOrderValue,
        COUNT(DISTINCT CustomerID) as UniqueCustomers
    FROM ecommerce
    GROUP BY Country
    ORDER BY TotalRevenue DESC
    LIMIT 15
""")

print("Top 15 Countries:")
sales_per_country.show()

# Save to Parquet
print("Saving to Parquet...")
sales_per_country.write.mode("overwrite").parquet("hdfs://namenode:9000/output/sales_per_country")
print("✓ Saved to: /output/sales_per_country")

# Step 7: Query 4 - Top Customers
print("\n[7/7] Query 4: Top 20 Customers...")
top_customers = spark.sql("""
    SELECT 
        CustomerID,
        Country,
        COUNT(DISTINCT InvoiceNo) as TotalOrders,
        SUM(Quantity) as TotalQuantity,
        SUM(TotalPrice) as TotalSpent,
        AVG(TotalPrice) as AvgOrderValue
    FROM ecommerce
    GROUP BY CustomerID, Country
    ORDER BY TotalSpent DESC
    LIMIT 20
""")

print("Top 20 Customers:")
top_customers.show()

# Save to Parquet
print("Saving to Parquet...")
top_customers.write.mode("overwrite").parquet("hdfs://namenode:9000/output/top_customers")
print("✓ Saved to: /output/top_customers")

# Summary Statistics
print("\n" + "="*70)
print("SUMMARY STATISTICS")
print("="*70)

summary = spark.sql("""
    SELECT 
        COUNT(DISTINCT InvoiceNo) as TotalInvoices,
        COUNT(DISTINCT CustomerID) as TotalCustomers,
        COUNT(DISTINCT Country) as TotalCountries,
        COUNT(DISTINCT ProductCategory) as TotalCategories,
        SUM(Quantity) as TotalItemsSold,
        SUM(TotalPrice) as TotalRevenue,
        AVG(TotalPrice) as AvgTransactionValue,
        MIN(InvoiceDate) as FirstTransaction,
        MAX(InvoiceDate) as LastTransaction
    FROM ecommerce
""")

summary.show(truncate=False)

# Verify all outputs in HDFS
print("\n" + "="*70)
print("VERIFYING OUTPUTS IN HDFS")
print("="*70)
os.system("docker exec namenode hdfs dfs -ls /output/")

print("\n" + "="*70)
print("SPARK PROCESSING COMPLETE!")
print("="*70)

# Stop Spark session
spark.stop()