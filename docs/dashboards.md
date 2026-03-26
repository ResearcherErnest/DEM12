# Metabase Sales Overview Dashboard Queries

Below are the exact SQL queries to set up the 5 required dashboards. These queries are designed to be run as "Native queries" in Metabase against the `sales` PostgreSQL database.

### 1. Monthly Revenue Trend
Visualizes the total revenue over time, grouped by month.
**Chart Type:** Bar/Line Chart
**X-Axis:** `month`
**Y-Axis:** `monthly_revenue`

```sql
SELECT 
    DATE_TRUNC('month', order_date) AS month, 
    SUM(total_revenue) AS monthly_revenue
FROM orders
WHERE status = 'completed'
GROUP BY month
ORDER BY month ASC;
```

### 2. Top Products
Highlights the top-performing products by total revenue across all time.
**Chart Type:** Horizontal Bar Chart
**X-Axis:** `total_revenue`
**Y-Axis:** `product_name`

```sql
SELECT 
    product_name, 
    total_revenue, 
    total_units_sold 
FROM purchased_products
ORDER BY total_revenue DESC
LIMIT 10;
```

### 3. Customer Insights
Shows the top customers by their lifetime value to identify VIPs.
**Chart Type:** Table or Horizontal Bar Chart
**X-Axis:** `lifetime_value`
**Y-Axis:** `name` / `email`

```sql
SELECT 
    name, 
    email, 
    region, 
    signup_date, 
    lifetime_value
FROM customers
ORDER BY lifetime_value DESC
LIMIT 20;
```

### 4. Sales by Region
Illustrates the distribution of total completed revenue across different geographic regions.
**Chart Type:** Pie Chart or Map
**Dimension:** `region`
**Measure:** `revenue`

```sql
SELECT 
    region, 
    SUM(total_revenue) AS revenue
FROM orders
WHERE status = 'completed'
GROUP BY region
ORDER BY revenue DESC;
```

### 5. Return Rate
Three Number (KPI) cards showing overall return rate, total returns, and total completed orders — each as a single bold metric on the dashboard.

#### 5a. Overall Return Rate (%)
**Chart Type:** Number card
**Display value:** `return_rate_pct`

```sql
SELECT
    ROUND(
        COUNT(DISTINCT r.order_id)::NUMERIC
        / NULLIF(COUNT(DISTINCT o.order_id), 0) * 100,
        2
    ) AS return_rate_pct
FROM orders o
LEFT JOIN returned_orders r ON r.order_id = o.order_id
WHERE o.status = 'completed';
```

#### 5b. Total Returned Orders
**Chart Type:** Number card
**Display value:** `total_returns`

```sql
SELECT COUNT(*) AS total_returns
FROM returned_orders;
```

#### 5c. Total Completed Orders
**Chart Type:** Number card
**Display value:** `total_orders`

```sql
SELECT COUNT(*) AS total_orders
FROM orders
WHERE status = 'completed';
```

#### 5d. Monthly Return Rate Trend *(optional — Line Chart)*
**Chart Type:** Line Chart
**X-Axis:** `month`
**Y-Axis:** `return_rate_pct`

```sql
SELECT
    DATE_TRUNC('month', o.order_date)           AS month,
    COUNT(DISTINCT o.order_id)                   AS total_orders,
    COUNT(DISTINCT r.order_id)                   AS total_returns,
    ROUND(
        COUNT(DISTINCT r.order_id)::NUMERIC
        / NULLIF(COUNT(DISTINCT o.order_id), 0) * 100,
        2
    )                                            AS return_rate_pct
FROM orders o
LEFT JOIN returned_orders r ON r.order_id = o.order_id
WHERE o.status = 'completed'
GROUP BY month
ORDER BY month ASC;
```

### 6. Data Quality and Pipeline Log
Displays the recent ETL pipeline runs, allowing monitoring of inserted rows versus skipped/invalid rows.
**Chart Type:** Table
**Columns:** `started_at`, `status`, `rows_inserted`, `rows_skipped`

```sql
SELECT 
    run_id,
    started_at,
    finished_at,
    dag_run_id,
    file_processed,
    status,
    rows_inserted,
    rows_skipped
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 50;
```
