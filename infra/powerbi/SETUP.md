# Power BI Desktop — ClickHouse Connection Setup

## Prerequisites
- Power BI Desktop (free): https://powerbi.microsoft.com/desktop/
- ClickHouse ODBC Driver: https://github.com/ClickHouse/clickhouse-odbc/releases

## Step 1: Install ClickHouse ODBC Driver
1. Download `clickhouse-odbc-*-win64.msi` from the releases page above.
2. Run the installer. It registers both 32-bit and 64-bit DSNs automatically.

## Step 2: Create an ODBC Data Source
1. Open **ODBC Data Sources (64-bit)** from the Start menu.
2. Click **System DSN** → **Add** → select **ClickHouse ODBC Driver**.
3. Fill in:
   - **DSN Name**: `ETL_ClickHouse`
   - **Host**: `localhost`
   - **Port**: `8123`
   - **Database**: `etl_warehouse`
   - **Username**: `etl_user`
   - **Password**: `etl_pass_2024`
4. Click **Test** → should show "Connection successful". Click **OK**.

## Step 3: Connect Power BI to ClickHouse
1. Open Power BI Desktop.
2. **Home** → **Get Data** → **More…** → search **ODBC** → click **Connect**.
3. Select **ETL_ClickHouse** from the DSN dropdown → **OK**.
4. Expand **etl_warehouse** in the Navigator → check the tables you want:
   - `agg_daily_revenue` — revenue by region/category
   - `agg_hourly_sensor` — IoT sensor trends + anomalies
   - `agg_trading_volume` — trading by asset
   - `fact_sales`, `fact_trades`, `dim_customer`, `dim_product`
5. Click **Load** (or **Transform Data** for custom queries).

## Step 4: Enable DirectQuery (optional, for live data)
1. In **Get Data** → ODBC → Advanced Options → enter a custom SQL query.
2. In the Model view, set table storage to **DirectQuery** for real-time data.

## Pre-built Reports
The `reports/` folder (once generated) contains `.pbix` files:
- `sales_overview.pbix` — revenue by region, top products, order status
- `iot_monitoring.pbix` — sensor readings, anomaly heatmap by location
- `trading_dashboard.pbix` — asset volumes, buy/sell ratio, price trends
- `social_engagement.pbix` — platform comparison, sentiment breakdown

## Connection String (for custom scripts)
```
Driver={ClickHouse ODBC Driver};
Host=localhost;Port=8123;Database=etl_warehouse;
UID=etl_user;PWD=etl_pass_2024;
```
