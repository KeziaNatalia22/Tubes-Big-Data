import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from glob import glob

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

os.makedirs('../output/visualizations', exist_ok=True)

print("="*70)
print("CREATING VISUALIZATIONS")
print("="*70)

def read_parquet_folder(folder_path):
    parquet_files = glob(f"{folder_path}/*.parquet")
    if not parquet_files:
        print(f"Warning: No parquet files found in {folder_path}")
        return None
    df_list = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(df_list, ignore_index=True)

# ===== VISUALIZATION 1: Sales Trend per Month =====
print("\n[1/5] Creating Sales Trend per Month...")
sales_month = read_parquet_folder('../output/results/sales_per_month')

if sales_month is not None:
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('E-Commerce Sales Analysis - Monthly Trends', fontsize=16, fontweight='bold')
    
    # Revenue trend
    axes[0, 0].plot(sales_month['YearMonth'], sales_month['TotalRevenue'], 
                    marker='o', linewidth=2, color='#2E86AB')
    axes[0, 0].set_title('Monthly Revenue Trend', fontweight='bold')
    axes[0, 0].set_xlabel('Month')
    axes[0, 0].set_ylabel('Total Revenue (£)')
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 0].grid(True, alpha=0.3)
    
    # Orders trend
    axes[0, 1].plot(sales_month['YearMonth'], sales_month['TotalOrders'], 
                    marker='s', linewidth=2, color='#A23B72')
    axes[0, 1].set_title('Monthly Orders Trend', fontweight='bold')
    axes[0, 1].set_xlabel('Month')
    axes[0, 1].set_ylabel('Total Orders')
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 1].grid(True, alpha=0.3)
    
    # Quantity sold
    axes[1, 0].bar(sales_month['YearMonth'], sales_month['TotalQuantity'], color='#F18F01')
    axes[1, 0].set_title('Monthly Quantity Sold', fontweight='bold')
    axes[1, 0].set_xlabel('Month')
    axes[1, 0].set_ylabel('Total Quantity')
    axes[1, 0].tick_params(axis='x', rotation=45)
    axes[1, 0].grid(True, alpha=0.3)
    
    # Unique customers
    axes[1, 1].plot(sales_month['YearMonth'], sales_month['UniqueCustomers'], 
                    marker='d', linewidth=2, color='#6A994E')
    axes[1, 1].set_title('Monthly Unique Customers', fontweight='bold')
    axes[1, 1].set_xlabel('Month')
    axes[1, 1].set_ylabel('Unique Customers')
    axes[1, 1].tick_params(axis='x', rotation=45)
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('../output/visualizations/01_monthly_trends.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 01_monthly_trends.png")
    plt.close()

# ===== VISUALIZATION 2: Top Product Categories =====
print("\n[2/5] Creating Top Product Categories...")
sales_category = read_parquet_folder('../output/results/sales_per_category')

if sales_category is not None:
    # Limit to top 15 for better visualization
    sales_category_top = sales_category.head(15)
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Top 15 Product Categories Performance', fontsize=16, fontweight='bold')
    
    # Revenue by category
    axes[0].barh(sales_category_top['ProductCategory'], 
                 sales_category_top['TotalRevenue'], color='#2E86AB')
    axes[0].set_title('Total Revenue by Category', fontweight='bold')
    axes[0].set_xlabel('Total Revenue (£)')
    axes[0].set_ylabel('Product Category')
    axes[0].invert_yaxis()
    
    # Quantity sold by category
    axes[1].barh(sales_category_top['ProductCategory'], 
                 sales_category_top['TotalQuantitySold'], color='#F18F01')
    axes[1].set_title('Total Quantity Sold by Category', fontweight='bold')
    axes[1].set_xlabel('Total Quantity Sold')
    axes[1].set_ylabel('Product Category')
    axes[1].invert_yaxis()
    
    plt.tight_layout()
    plt.savefig('../output/visualizations/02_top_categories.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 02_top_categories.png")
    plt.close()

# ===== VISUALIZATION 3: Top Countries =====
print("\n[3/5] Creating Top Countries Analysis...")
sales_country = read_parquet_folder('../output/results/sales_per_country')

if sales_country is not None:
    # Top 10 countries
    sales_country_top = sales_country.head(10)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Top 10 Countries - E-Commerce Performance', fontsize=16, fontweight='bold')
    
    # Revenue by country
    axes[0, 0].bar(sales_country_top['Country'], sales_country_top['TotalRevenue'], 
                   color='#2E86AB')
    axes[0, 0].set_title('Revenue by Country', fontweight='bold')
    axes[0, 0].set_ylabel('Total Revenue (£)')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # Orders by country
    axes[0, 1].bar(sales_country_top['Country'], sales_country_top['TotalOrders'], 
                   color='#A23B72')
    axes[0, 1].set_title('Orders by Country', fontweight='bold')
    axes[0, 1].set_ylabel('Total Orders')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # Unique customers by country
    axes[1, 0].bar(sales_country_top['Country'], sales_country_top['UniqueCustomers'], 
                   color='#6A994E')
    axes[1, 0].set_title('Unique Customers by Country', fontweight='bold')
    axes[1, 0].set_ylabel('Unique Customers')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # Avg order value by country
    axes[1, 1].bar(sales_country_top['Country'], sales_country_top['AvgOrderValue'], 
                   color='#F18F01')
    axes[1, 1].set_title('Average Order Value by Country', fontweight='bold')
    axes[1, 1].set_ylabel('Avg Order Value (£)')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('../output/visualizations/03_top_countries.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 03_top_countries.png")
    plt.close()

# ===== VISUALIZATION 4: Top Customers =====
print("\n[4/5] Creating Top Customers Analysis...")
top_customers = read_parquet_folder('../output/results/top_customers')

if top_customers is not None:
    # Top 15 customers
    top_customers_15 = top_customers.head(15)
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Top 15 Customers Performance', fontsize=16, fontweight='bold')
    
    # Convert CustomerID to string for better display
    top_customers_15['CustomerID'] = top_customers_15['CustomerID'].astype(str)
    
    # Total spent
    axes[0].barh(top_customers_15['CustomerID'], top_customers_15['TotalSpent'], 
                 color='#2E86AB')
    axes[0].set_title('Total Spending by Top Customers', fontweight='bold')
    axes[0].set_xlabel('Total Spent (£)')
    axes[0].set_ylabel('Customer ID')
    axes[0].invert_yaxis()
    
    # Total orders
    axes[1].barh(top_customers_15['CustomerID'], top_customers_15['TotalOrders'], 
                 color='#A23B72')
    axes[1].set_title('Total Orders by Top Customers', fontweight='bold')
    axes[1].set_xlabel('Total Orders')
    axes[1].set_ylabel('Customer ID')
    axes[1].invert_yaxis()
    
    plt.tight_layout()
    plt.savefig('../output/visualizations/04_top_customers.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 04_top_customers.png")
    plt.close()

# ===== VISUALIZATION 5: Dashboard Summary =====
print("\n[5/5] Creating Summary Dashboard...")

if all([sales_month is not None, sales_category is not None, 
        sales_country is not None, top_customers is not None]):
    
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    fig.suptitle('E-Commerce Analytics Dashboard - Overall Summary', 
                 fontsize=18, fontweight='bold')
    
    # KPI Cards (as text)
    ax_kpi = fig.add_subplot(gs[0, :])
    ax_kpi.axis('off')
    
    total_revenue = sales_month['TotalRevenue'].sum()
    total_orders = sales_month['TotalOrders'].sum()
    total_customers = top_customers['CustomerID'].nunique()
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    kpi_text = f"""
    OVERALL PERFORMANCE METRICS
    
    Total Revenue: £{total_revenue:,.2f}  |  Total Orders: {total_orders:,}  |  
    Unique Customers: {total_customers:,}  |  Avg Order Value: £{avg_order_value:.2f}
    """
    
    ax_kpi.text(0.5, 0.5, kpi_text, ha='center', va='center', 
                fontsize=14, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    # Revenue trend
    ax1 = fig.add_subplot(gs[1, :])
    ax1.plot(sales_month['YearMonth'], sales_month['TotalRevenue'], 
             marker='o', linewidth=2, color='#2E86AB')
    ax1.set_title('Monthly Revenue Trend', fontweight='bold')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Revenue (£)')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # Top 5 categories
    ax2 = fig.add_subplot(gs[2, 0])
    top5_cat = sales_category.head(5)
    ax2.barh(top5_cat['ProductCategory'], top5_cat['TotalRevenue'], color='#F18F01')
    ax2.set_title('Top 5 Categories', fontweight='bold')
    ax2.set_xlabel('Revenue (£)')
    ax2.invert_yaxis()
    
    # Top 5 countries
    ax3 = fig.add_subplot(gs[2, 1])
    top5_country = sales_country.head(5)
    ax3.bar(top5_country['Country'], top5_country['TotalRevenue'], color='#A23B72')
    ax3.set_title('Top 5 Countries', fontweight='bold')
    ax3.set_ylabel('Revenue (£)')
    ax3.tick_params(axis='x', rotation=45)
    
    # Revenue distribution pie chart
    ax4 = fig.add_subplot(gs[2, 2])
    top5_country = sales_country.head(5)
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#6A994E', '#BC4B51']
    ax4.pie(top5_country['TotalRevenue'], labels=top5_country['Country'], 
            autopct='%1.1f%%', colors=colors)
    ax4.set_title('Revenue Share - Top 5 Countries', fontweight='bold')
    
    plt.savefig('../output/visualizations/05_dashboard_summary.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 05_dashboard_summary.png")
    plt.close()
