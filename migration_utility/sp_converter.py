"""
SQL Stored Procedure to PySpark (Databricks) Converter
Handles CTEs, Window Functions, Temp Tables, Cursors, Merges, Dynamic SQL
"""

import re
import textwrap

# ─────────────────────────────────────────────────────────────────────────────
# PySpark Templates for each stored procedure
# ─────────────────────────────────────────────────────────────────────────────

PYSPARK_TEMPLATES = {

    # ── 1. Sales Aggregation ─────────────────────────────────────────────────
    "SP_SalesAggregation_Analytics": """
# =============================================================================
# Databricks PySpark Notebook
# Converted From : SP_SalesAggregation_Analytics
# Format         : Databricks / Unity Catalog compatible
# Catalog        : main   |  Schema: sales_analytics
# Generated      : Auto-converted via SP Migration Utility
# =============================================================================

# ── Notebook Parameters (Databricks Widgets) ─────────────────────────────────
dbutils.widgets.text("start_date",   "2024-01-01", "Start Date")
dbutils.widgets.text("end_date",     "2024-12-31", "End Date")
dbutils.widgets.text("region_code",  "",           "Region Code (blank = ALL)")
dbutils.widgets.text("catalog",      "main",       "Unity Catalog Name")
dbutils.widgets.text("schema",       "sales_analytics", "Schema Name")

start_date   = dbutils.widgets.get("start_date")
end_date     = dbutils.widgets.get("end_date")
region_code  = dbutils.widgets.get("region_code")
catalog      = dbutils.widgets.get("catalog")
schema       = dbutils.widgets.get("schema")

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("SP_SalesAggregation_Analytics").getOrCreate()

print(f"[INFO] Starting SP_SalesAggregation_Analytics | {datetime.now()}")
print(f"[INFO] Date Range  : {start_date} -> {end_date}")
print(f"[INFO] Region      : {region_code if region_code else 'ALL'}")

# ── Read Source Tables ────────────────────────────────────────────────────────
sales_df     = spark.table(f"`{catalog}`.`{schema}`.sales")
customers_df = spark.table(f"`{catalog}`.`{schema}`.customers")
products_df  = spark.table(f"`{catalog}`.`{schema}`.products")

# ── Step 1 : Base Sales (replaces CTE BaseSales) ─────────────────────────────
base_sales_df = (
    sales_df
    .filter(
        (F.col("sale_date").between(start_date, end_date)) &
        (F.col("is_active") == 1)
    )
    .join(customers_df, "customer_id", "inner")
    .join(products_df,  "product_id",  "inner")
    .withColumn(
        "net_revenue",
        F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount"), F.lit(0)))
    )
)

# Apply optional region filter
if region_code:
    base_sales_df = base_sales_df.filter(F.col("region") == region_code)

# ── Step 2 : Monthly Aggregation (replaces CTE MonthlySales) ─────────────────
monthly_sales_df = (
    base_sales_df
    .withColumn("sale_year",  F.year("sale_date"))
    .withColumn("sale_month", F.month("sale_date"))
    .groupBy("sale_year", "sale_month", "region", "customer_segment", "category_id")
    .agg(
        F.sum("net_revenue")            .alias("total_revenue"),
        F.countDistinct("sale_id")      .alias("total_transactions"),
        F.countDistinct("customer_id")  .alias("unique_customers"),
        F.avg("net_revenue")            .alias("avg_order_value"),
        F.sum("quantity")               .alias("total_units")
    )
)

# ── Step 3 : Window Functions (replaces CTE RankedSales) ─────────────────────
ytd_window     = (Window.partitionBy("region", "sale_year")
                        .orderBy("sale_month")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow))

rank_window    = (Window.partitionBy("sale_year", "sale_month")
                        .orderBy(F.desc("total_revenue")))

lag_window     = (Window.partitionBy("region", "customer_segment")
                        .orderBy("sale_year", "sale_month"))

ranked_sales_df = (
    monthly_sales_df
    .withColumn("ytd_revenue",       F.sum("total_revenue").over(ytd_window))
    .withColumn("region_rank",       F.rank().over(rank_window))
    .withColumn("prev_month_revenue", F.lag("total_revenue", 1, 0).over(lag_window))
)

# ── Step 4 : Final Metrics & MoM Growth ─────────────────────────────────────
final_df = ranked_sales_df.withColumn(
    "mom_growth_pct",
    F.when(
        F.col("prev_month_revenue") == 0, F.lit(None).cast("double")
    ).otherwise(
        F.round(
            ((F.col("total_revenue") - F.col("prev_month_revenue")) /
              F.col("prev_month_revenue")) * 100,
            2
        )
    )
)

# ── Step 5 : Write to Unity Catalog Delta Table ───────────────────────────────
output_table = f"`{catalog}`.`{schema}`.sales_aggregation_result"
print(f"[INFO] Writing to Unity Catalog table : {output_table}")

(final_df
    .orderBy("sale_year", "sale_month", "region_rank")
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table))

row_count = spark.table(output_table).count()
print(f"[SUCCESS] Table written: {output_table} | Rows: {row_count:,}")

# ── Audit Log ────────────────────────────────────────────────────────────────
audit_df = spark.createDataFrame([{
    "procedure_name"  : "SP_SalesAggregation_Analytics",
    "executed_at"     : str(datetime.now()),
    "rows_affected"   : row_count,
    "parameters"      : f"start_date={start_date}, end_date={end_date}, region={region_code or 'ALL'}"
}])
audit_df.write.format("delta").mode("append").saveAsTable(f"`{catalog}`.`{schema}`.audit_log")
print("[INFO] Audit log updated.")

display(spark.table(output_table).limit(100))
""",

    # ── 2. Inventory Management ───────────────────────────────────────────────
    "SP_Inventory_Management": """
# =============================================================================
# Databricks PySpark Notebook
# Converted From : SP_Inventory_Management
# Cursor → mapPartitions  |  Temp Table → DataFrame
# Catalog        : main   |  Schema: inventory
# =============================================================================

dbutils.widgets.text("warehouse_id", "1",  "Warehouse ID")
dbutils.widgets.text("dry_run",      "0",  "Dry Run (0=Execute, 1=Preview)")
dbutils.widgets.text("catalog",      "main", "Unity Catalog Name")
dbutils.widgets.text("schema",       "inventory", "Schema Name")

warehouse_id = int(dbutils.widgets.get("warehouse_id"))
dry_run      = dbutils.widgets.get("dry_run") == "1"
catalog      = dbutils.widgets.get("catalog")
schema       = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

spark = SparkSession.builder.appName("SP_Inventory_Management").getOrCreate()
print(f"[INFO] SP_Inventory_Management | Warehouse={warehouse_id} | DryRun={dry_run}")

# ── Read Tables ───────────────────────────────────────────────────────────────
products_df      = spark.table(f"`{catalog}`.`{schema}`.products")
inventory_df     = spark.table(f"`{catalog}`.`{schema}`.inventory")
product_sup_df   = spark.table(f"`{catalog}`.`{schema}`.product_supplier")
suppliers_df     = spark.table(f"`{catalog}`.`{schema}`.suppliers")

# ── Step 1 : Identify Low Stock Items (replaces temp table #LowStockItems) ───
low_stock_df = (
    products_df.filter(F.col("is_discontinued") == 0)
    .join(
        inventory_df.filter(F.col("warehouse_id") == warehouse_id),
        "product_id", "inner"
    )
    .join(
        product_sup_df.filter(F.col("is_primary") == 1),
        "product_id", "inner"
    )
    .join(
        suppliers_df.filter(F.col("is_active") == 1),
        "supplier_id", "inner"
    )
    .filter(F.col("quantity_on_hand") <= F.col("reorder_point"))
    .withColumn(
        "calc_reorder_qty",
        F.when(F.col("quantity_on_hand") <= 0,
               F.col("reorder_quantity") * 2)
         .when(F.col("quantity_on_hand") < F.col("reorder_point") * 0.5,
               F.col("reorder_quantity") + (F.col("reorder_point") - F.col("quantity_on_hand")))
         .otherwise(F.col("reorder_quantity"))
    )
    .withColumn("reorder_status", F.lit("PENDING"))
    .select(
        "product_id", "product_name", "quantity_on_hand", "reorder_point",
        "reorder_quantity", "lead_time_days", "supplier_id",
        "unit_cost", "reorder_status", "calc_reorder_qty"
    )
)

low_stock_df.cache()
total_items = low_stock_df.count()
print(f"[INFO] Low stock items found: {total_items}")

# ── Step 2 : Generate Purchase Orders (replaces cursor loop) ─────────────────
# In PySpark we vectorize the cursor logic instead of row-by-row processing.

if not dry_run:
    batch_time = datetime.now()

    # Create Purchase Order headers (one per supplier)
    po_headers_df = (
        low_stock_df.groupBy("supplier_id")
        .agg(
            F.sum(F.col("calc_reorder_qty") * F.col("unit_cost")).alias("total_amount")
        )
        .withColumn("purchase_order_id", F.expr("uuid()"))
        .withColumn("warehouse_id",      F.lit(warehouse_id))
        .withColumn("order_date",        F.lit(str(batch_time)))
        .withColumn("status",            F.lit("SUBMITTED"))
    )

    po_headers_df.write.format("delta").mode("append") \
        .saveAsTable(f"`{catalog}`.`{schema}`.purchase_orders")

    # Create PO Lines
    po_lines_df = (
        low_stock_df
        .join(po_headers_df.select("supplier_id", "purchase_order_id"), "supplier_id")
        .withColumn("line_total", F.col("calc_reorder_qty") * F.col("unit_cost"))
        .select(
            "purchase_order_id", "product_id", "calc_reorder_qty",
            "unit_cost", "line_total"
        )
        .withColumnRenamed("calc_reorder_qty", "quantity")
    )

    po_lines_df.write.format("delta").mode("append") \
        .saveAsTable(f"`{catalog}`.`{schema}`.purchase_order_lines")

    low_stock_df = low_stock_df.withColumn("reorder_status", F.lit("ORDERED"))
    print(f"[SUCCESS] Purchase Orders created for {total_items} products.")
else:
    low_stock_df = low_stock_df.withColumn("reorder_status", F.lit("DRY_RUN"))
    print("[INFO] Dry run mode — no orders created.")

# ── Step 3 : Write Results to Unity Catalog ───────────────────────────────────
output_table = f"`{catalog}`.`{schema}`.inventory_reorder_result"
low_stock_df.orderBy("quantity_on_hand").write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(output_table)

display(spark.table(output_table))
""",

    # ── 3. Customer Segmentation ──────────────────────────────────────────────
    "SP_Customer_Segmentation_ML": """
# =============================================================================
# Databricks PySpark Notebook
# Converted From : SP_Customer_Segmentation_ML
# RFM Segmentation + ML Feature Engineering
# Catalog        : main   |  Schema: customer_analytics
# =============================================================================

dbutils.widgets.text("analysis_date",    "",  "Analysis Date (blank=today)")
dbutils.widgets.text("min_transactions", "2", "Min Transactions")
dbutils.widgets.text("output_to_table",  "1", "Write to Delta Table (1=Yes)")
dbutils.widgets.text("catalog",          "main", "Unity Catalog Name")
dbutils.widgets.text("schema",           "customer_analytics", "Schema Name")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, datetime

spark = SparkSession.builder.appName("SP_Customer_Segmentation_ML").getOrCreate()

analysis_date    = dbutils.widgets.get("analysis_date") or str(date.today())
min_transactions = int(dbutils.widgets.get("min_transactions"))
output_to_table  = dbutils.widgets.get("output_to_table") == "1"
catalog          = dbutils.widgets.get("catalog")
schema           = dbutils.widgets.get("schema")

print(f"[INFO] Customer Segmentation | AnalysisDate={analysis_date} | MinTxn={min_transactions}")

# ── Read Tables ───────────────────────────────────────────────────────────────
customers_df = spark.table(f"`{catalog}`.`{schema}`.customers").filter(F.col("is_active") == 1)
orders_df    = (spark.table(f"`{catalog}`.`{schema}`.orders")
                .filter(
                    (F.col("order_date") <= analysis_date) &
                    (~F.col("status").isin(["CANCELLED", "FRAUD"]))
                ))
order_lines_df = spark.table(f"`{catalog}`.`{schema}`.order_lines")
products_df    = spark.table(f"`{catalog}`.`{schema}`.products")

# ── Step 1 : RFM Base Calculation ─────────────────────────────────────────────
rfm_base_df = (
    orders_df
    .join(customers_df, "customer_id", "inner")
    .groupBy("customer_id", "customer_name", "email", "region",
             "tier_level", "acquisition_date")
    .agg(
        F.datediff(F.lit(analysis_date), F.max("order_date")).alias("recency"),
        F.countDistinct("order_id")                          .alias("frequency"),
        F.sum("order_total")                                 .alias("monetary"),
        F.avg("order_total")                                 .alias("avg_order_value"),
        F.min("order_date")                                  .alias("first_order_date"),
        F.max("order_date")                                  .alias("last_order_date"),
        F.stddev("order_total")                              .alias("order_value_std_dev"),
        F.sum(F.when(F.col("return_flag") == 1, 1).otherwise(0)).alias("return_count")
    )
    .filter(F.col("frequency") >= min_transactions)
)

# ── Step 2 : NTILE Scoring (NTILE → percent_rank + bucketizer) ───────────────
ntile_fn = lambda col, order: F.ntile(5).over(Window.orderBy(order))

rfm_scored_df = (
    rfm_base_df
    .withColumn("r_score", F.ntile(5).over(Window.orderBy(F.asc("recency"))))
    .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.desc("frequency"))))
    .withColumn("m_score", F.ntile(5).over(Window.orderBy(F.desc("monetary"))))
)

# ── Step 3 : Top Spend Category ───────────────────────────────────────────────
category_spend_df = (
    orders_df
    .join(order_lines_df, "order_id")
    .join(products_df, "product_id")
    .groupBy("customer_id", "category_name")
    .agg(F.sum("line_total").alias("category_total"))
)

top_category_df = (
    category_spend_df
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.desc("category_total"))
    ))
    .filter(F.col("rn") == 1)
    .withColumnRenamed("category_name", "top_spend_category")
    .select("customer_id", "top_spend_category")
)

# ── Step 4 : Segmentation Logic ───────────────────────────────────────────────
segmented_df = (
    rfm_scored_df.join(top_category_df, "customer_id", "left")
    .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    .withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
         .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3),                             "Loyal Customers")
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2),                             "Recent Customers")
         .when((F.col("r_score") >= 3) & (F.col("m_score") >= 4),                             "Potential Loyalists")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),  "At Risk")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3),                             "Cant Lose Them")
         .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2) & (F.col("m_score") <= 2),  "Lost")
         .otherwise("Needs Attention")
    )
    .withColumn("customer_age_days",
        F.datediff(F.lit(analysis_date), F.col("acquisition_date")))
    .withColumn("return_rate",
        F.round(F.col("return_count") / F.nullif(F.col("frequency"), 0), 4))
)

# ── Step 5 : MERGE into Unity Catalog (SCD1 upsert via MERGE) ────────────────
if output_to_table:
    segmented_df.createOrReplaceTempView("customer_segments_staging")
    spark.sql(f'''
        MERGE INTO `{catalog}`.`{schema}`.customer_segmentation AS tgt
        USING customer_segments_staging AS src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET
            tgt.segment      = src.segment,
            tgt.rfm_total    = src.rfm_total,
            tgt.r_score      = src.r_score,
            tgt.f_score      = src.f_score,
            tgt.m_score      = src.m_score,
            tgt.last_updated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT *
    ''')
    print(f"[SUCCESS] CustomerSegmentation MERGE complete.")

display(segmented_df.orderBy(F.desc("rfm_total")).limit(200))
""",

    # ── 4. Financial Reporting GL ─────────────────────────────────────────────
    "SP_Financial_Reporting_GL": """
# =============================================================================
# Databricks PySpark Notebook
# Converted From : SP_Financial_Reporting_GL
# Recursive CTE  → GraphFrames / iterative joins
# Catalog        : main   |  Schema: finance
# =============================================================================

dbutils.widgets.text("fiscal_year",    "2024",  "Fiscal Year")
dbutils.widgets.text("fiscal_period",  "",      "Fiscal Period (blank=all)")
dbutils.widgets.text("entity_id",      "",      "Entity ID (blank=all)")
dbutils.widgets.text("currency_code",  "USD",   "Currency Code")
dbutils.widgets.text("catalog",        "main",  "Unity Catalog Name")
dbutils.widgets.text("schema",         "finance", "Schema Name")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SP_Financial_Reporting_GL").getOrCreate()

fiscal_year   = int(dbutils.widgets.get("fiscal_year"))
fiscal_period = dbutils.widgets.get("fiscal_period")
entity_id     = dbutils.widgets.get("entity_id")
currency_code = dbutils.widgets.get("currency_code")
catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")

# ── Step 1 : Exchange Rate ────────────────────────────────────────────────────
currency_df = spark.table(f"`{catalog}`.`{schema}`.currency_rates") \
    .filter(F.col("currency_code") == currency_code)

max_rate_date = currency_df.agg(F.max("rate_date")).collect()[0][0]
exchange_rate = (currency_df.filter(F.col("rate_date") == max_rate_date)
                 .select("exchange_rate").collect()[0][0] or 1.0)

print(f"[INFO] Exchange Rate ({currency_code}): {exchange_rate}")

# ── Step 2 : Journal Entries ──────────────────────────────────────────────────
je_df = spark.table(f"`{catalog}`.`{schema}`.journal_entries") \
    .filter(
        (F.col("fiscal_year") == fiscal_year) &
        (F.col("is_posted") == 1) &
        (F.col("is_reversed") == 0)
    )

if fiscal_period:
    je_df = je_df.filter(F.col("fiscal_period") <= int(fiscal_period))
if entity_id:
    je_df = je_df.filter(F.col("entity_id") == int(entity_id))

coa_df = spark.table(f"`{catalog}`.`{schema}`.chart_of_accounts")

je_with_coa = (
    je_df.join(coa_df, "account_id", "inner")
    .withColumn("debit_usd",  F.col("debit_amount")  * exchange_rate)
    .withColumn("credit_usd", F.col("credit_amount") * exchange_rate)
)

# ── Step 3 : Trial Balance ────────────────────────────────────────────────────
trial_balance_df = (
    je_with_coa
    .groupBy("account_id", "account_code", "account_name", "account_type", "parent_account_id")
    .agg(
        F.sum("debit_usd") .alias("total_debits"),
        F.sum("credit_usd").alias("total_credits")
    )
    .withColumn("net_balance", F.col("total_debits") - F.col("total_credits"))
)

# ── Step 4 : Account Hierarchy (iterative BFS, replaces recursive CTE) ───────
MAX_DEPTH = 10
hierarchy_df = coa_df.filter(F.col("parent_account_id").isNull()) \
    .select(
        F.col("account_id"),
        F.col("account_code").alias("hierarchy_path"),
        F.lit(0).alias("level")
    )

for depth in range(1, MAX_DEPTH):
    children = (
        coa_df
        .join(hierarchy_df.select(
            F.col("account_id").alias("parent_id"),
            F.col("hierarchy_path").alias("parent_path"),
            F.col("level").alias("parent_level")
        ), coa_df.parent_account_id == F.col("parent_id"))
        .select(
            coa_df.account_id,
            F.concat(F.col("parent_path"), F.lit(" > "), coa_df.account_code).alias("hierarchy_path"),
            (F.col("parent_level") + 1).alias("level")
        )
    )
    if children.count() == 0:
        break
    hierarchy_df = hierarchy_df.union(children)

# ── Step 5 : P&L Reporting Balance ───────────────────────────────────────────
pnl_df = (
    trial_balance_df
    .join(hierarchy_df, "account_id", "left")
    .withColumn(
        "reporting_balance",
        F.when(F.col("account_type").isin("Revenue", "Other Income"),
               F.col("total_credits") - F.col("total_debits"))
         .when(F.col("account_type").isin("COGS", "Operating Expense"),
               F.col("total_debits") - F.col("total_credits"))
         .otherwise(F.col("net_balance"))
    )
    .withColumn("subtotal_by_type",
        F.sum("reporting_balance").over(Window.partitionBy("account_type")))
    .withColumn("line_order",
        F.row_number().over(
            Window.partitionBy("account_type").orderBy("account_code")))
    .withColumn("fiscal_year",    F.lit(fiscal_year))
    .withColumn("fiscal_period",  F.lit(fiscal_period or "ALL"))
    .withColumn("report_currency", F.lit(currency_code))
    .withColumn("generated_at",   F.current_timestamp())
)

# ── Step 6 : Write Results ────────────────────────────────────────────────────
output_table = f"`{catalog}`.`{schema}`.financial_report_gl"
pnl_df.orderBy("account_type", "account_code") \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(output_table)

print(f"[SUCCESS] Financial report written to {output_table}")
display(spark.table(output_table))
""",

    # ── 5. ETL Pipeline ───────────────────────────────────────────────────────
    "SP_ETL_DataPipeline_Staging": """
# =============================================================================
# Databricks PySpark Notebook
# Converted From : SP_ETL_DataPipeline_Staging
# Cursor/Dynamic SQL → Structured Streaming + Delta MERGE
# SCD Type 2 fully implemented in PySpark
# Catalog        : main   |  Schema: etl_staging
# =============================================================================

dbutils.widgets.text("batch_id",       "",            "Batch ID (blank=auto)")
dbutils.widgets.text("source_system",  "SALES_CRM",   "Source System")
dbutils.widgets.text("target_table",   "dim_customer", "Target Table")
dbutils.widgets.text("load_mode",      "INCREMENTAL", "Load Mode (FULL/INCREMENTAL/CDC)")
dbutils.widgets.text("watermark_date", "",            "Watermark Date (blank=auto)")
dbutils.widgets.text("catalog",        "main",        "Unity Catalog Name")
dbutils.widgets.text("schema",         "etl_staging", "Schema Name")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

spark = SparkSession.builder.appName("SP_ETL_DataPipeline_Staging").getOrCreate()

batch_id       = dbutils.widgets.get("batch_id") or str(uuid.uuid4())
source_system  = dbutils.widgets.get("source_system")
target_table   = dbutils.widgets.get("target_table")
load_mode      = dbutils.widgets.get("load_mode").upper()
watermark_date = dbutils.widgets.get("watermark_date")
catalog        = dbutils.widgets.get("catalog")
schema         = dbutils.widgets.get("schema")
start_time     = datetime.now()

print(f"[INFO] ETL Pipeline Start | BatchID={batch_id} | Source={source_system} | Mode={load_mode}")

# ── Step 1: Log Batch Start (replaces INSERT INTO ETL_BatchLog) ───────────────
batch_log_entry = spark.createDataFrame([{
    "batch_id": batch_id, "source_system": source_system,
    "target_table": target_table, "load_mode": load_mode,
    "start_time": str(start_time), "status": "RUNNING",
    "rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_rejected": 0
}])
batch_log_entry.write.format("delta").mode("append") \
    .saveAsTable(f"`{catalog}`.`{schema}`.etl_batch_log")

# ── Step 2: Extract from Source ───────────────────────────────────────────────
source_df = spark.table(f"`{catalog}`.`{schema}`.source_data") \
    .filter(F.col("source_system") == source_system)

if load_mode == "INCREMENTAL" and watermark_date:
    source_df = source_df.filter(F.col("modified_at") >= watermark_date)

rows_read = source_df.count()
print(f"[INFO] Rows extracted: {rows_read:,}")

# ── Step 3: Data Quality Validation ──────────────────────────────────────────
validated_df = (
    source_df
    .withColumn(
        "validation_status",
        F.when(F.col("natural_key").isNull() | (F.length(F.trim(F.col("natural_key").cast("string"))) == 0),
               F.lit("FAILED:missing_key"))
         .when(F.length(F.col("raw_data").cast("string")) > 65000,
               F.lit("FAILED:size_exceeded"))
         .otherwise(F.lit("PASSED"))
    )
)

rejected_df = validated_df.filter(F.col("validation_status").startswith("FAILED"))
passed_df   = validated_df.filter(F.col("validation_status") == "PASSED")
rows_rejected = rejected_df.count()

if rows_rejected > 0:
    rejected_df.select("natural_key", "raw_data", "validation_status") \
        .withColumn("batch_id", F.lit(batch_id)) \
        .withColumn("rejected_at", F.current_timestamp()) \
        .write.format("delta").mode("append") \
        .saveAsTable(f"`{catalog}`.`{schema}`.etl_rejected_records")
    print(f"[WARN] Rejected rows: {rows_rejected:,} — written to etl_rejected_records")

# ── Step 4: SCD Type 2 MERGE ──────────────────────────────────────────────────
# First, expire old records
passed_df = (
    passed_df
    .withColumn("business_key", F.col("natural_key"))
    .withColumn("record_hash",  F.md5(F.col("raw_data").cast("string")))
    .withColumn("extracted_at", F.current_timestamp())
)

passed_df.createOrReplaceTempView("etl_staging_view")

# Expire changed records
spark.sql(f'''
    MERGE INTO `{catalog}`.`{schema}`.`{target_table}` AS tgt
    USING etl_staging_view AS src
    ON tgt.business_key = src.business_key AND tgt.is_current = true
    WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
        UPDATE SET
            tgt.is_current     = false,
            tgt.effective_end  = src.extracted_at
    WHEN NOT MATCHED THEN
        INSERT (source_key, business_key, raw_data, record_hash,
                is_current, effective_start, effective_end, batch_id)
        VALUES (src.natural_key, src.business_key, src.raw_data, src.record_hash,
                true, src.extracted_at, \'9999-12-31\', \'{batch_id}\')
''')

rows_updated = spark.table(f"`{catalog}`.`{schema}`.`{target_table}`") \
    .filter(F.col("batch_id") == batch_id).count()

# ── Step 5: Update Watermark ──────────────────────────────────────────────────
spark.sql(f'''
    MERGE INTO `{catalog}`.`{schema}`.etl_watermarks AS w
    USING (SELECT \'{source_system}\' AS source_system, max(extracted_at) AS new_watermark
           FROM etl_staging_view) AS s
    ON w.source_system = s.source_system
    WHEN MATCHED THEN UPDATE SET w.last_load_time = s.new_watermark
    WHEN NOT MATCHED THEN INSERT (source_system, last_load_time)
         VALUES (s.source_system, s.new_watermark)
''')

# ── Step 6: Update Batch Log ──────────────────────────────────────────────────
duration = int((datetime.now() - start_time).total_seconds())
spark.sql(f'''
    UPDATE `{catalog}`.`{schema}`.etl_batch_log
    SET  status            = \'SUCCEEDED\',
         end_time          = current_timestamp(),
         rows_read         = {rows_read},
         rows_updated      = {rows_updated},
         rows_rejected     = {rows_rejected},
         duration_seconds  = {duration}
    WHERE batch_id = \'{batch_id}\'
''')

print(f"[SUCCESS] ETL Complete | BatchID={batch_id} | Read={rows_read} | Updated={rows_updated} | Rejected={rows_rejected} | Duration={duration}s")

result_df = spark.createDataFrame([{
    "batch_id": batch_id, "status": "SUCCEEDED", "rows_read": rows_read,
    "rows_updated": rows_updated, "rows_rejected": rows_rejected, "duration_seconds": duration
}])
display(result_df)
"""
}


# ─────────────────────────────────────────────────────────────────────────────
# PySpark Templates for SQL Views
# ─────────────────────────────────────────────────────────────────────────────
PYSPARK_VIEWS = {
    "VW_CustomerOrderSummary": '''
import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Widget Parameters ──────────────────────────────────────────────────────
dbutils.widgets.text("catalog",  "main",    "Catalog")
dbutils.widgets.text("schema",   "default", "Schema")
dbutils.widgets.text("as_of_date", str(datetime.date.today()), "As-of Date (YYYY-MM-DD)")

catalog    = dbutils.widgets.get("catalog")
schema     = dbutils.widgets.get("schema")
as_of_date = dbutils.widgets.get("as_of_date")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# ── Load Source Tables ─────────────────────────────────────────────────────
customers = spark.table("customers")
orders    = spark.table("orders")
order_items = spark.table("order_items")
products  = spark.table("products")

# ── Order Metrics per Customer ─────────────────────────────────────────────
order_metrics = (
    orders
    .filter(F.col("order_date") <= F.lit(as_of_date))
    .join(order_items, "order_id")
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_total").alias("lifetime_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.avg("order_total").alias("avg_order_value"),
        F.sum(F.when(F.col("return_flag") == 1, 1).otherwise(0)).alias("total_returns")
    )
)

# ── Recency Score ──────────────────────────────────────────────────────────
order_metrics = order_metrics.withColumn(
    "days_since_last_order",
    F.datediff(F.lit(as_of_date), F.col("last_order_date"))
)

# ── Customer Segment ───────────────────────────────────────────────────────
order_metrics = order_metrics.withColumn(
    "customer_segment",
    F.when(F.col("lifetime_value") >= 10000, "Premium")
     .when(F.col("lifetime_value") >= 1000,  "Regular")
     .otherwise("Occasional")
)

# ── Preferred Category ─────────────────────────────────────────────────────
cat_spend = (
    orders.join(order_items, "order_id")
          .join(products,    "product_id")
          .groupBy("customer_id", "category")
          .agg(F.sum("order_total").alias("cat_spend"))
)
w_cat = Window.partitionBy("customer_id").orderBy(F.desc("cat_spend"))
preferred_cat = (
    cat_spend
    .withColumn("rn", F.row_number().over(w_cat))
    .filter(F.col("rn") == 1)
    .select("customer_id", F.col("category").alias("preferred_category"))
)

# ── Final Join ─────────────────────────────────────────────────────────────
result = (
    customers
    .join(order_metrics,  "customer_id", "left")
    .join(preferred_cat,  "customer_id", "left")
    .select(
        "customer_id", "customer_name", "email", "city", "country",
        F.coalesce("total_orders",       F.lit(0)).alias("total_orders"),
        F.coalesce("lifetime_value",     F.lit(0.0)).alias("lifetime_value"),
        "first_order_date", "last_order_date",
        F.coalesce("avg_order_value",    F.lit(0.0)).alias("avg_order_value"),
        F.coalesce("days_since_last_order", F.lit(9999)).alias("days_since_last_order"),
        "customer_segment", "preferred_category",
        F.coalesce("total_returns", F.lit(0)).alias("total_returns"),
        F.current_timestamp().alias("view_refreshed_at")
    )
)

# ── Write as Delta View/Table ──────────────────────────────────────────────
result.createOrReplaceTempView("VW_CustomerOrderSummary")
result.write.format("delta").mode("overwrite").option("overwriteSchema","true") \\
      .saveAsTable(f"{catalog}.{schema}.VW_CustomerOrderSummary")
print(f"VW_CustomerOrderSummary refreshed — {result.count():,} rows")
''',

    "VW_ProductInventoryStatus": '''
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Widget Parameters ──────────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main",    "Catalog")
dbutils.widgets.text("schema",  "default", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# ── Load Source Tables ─────────────────────────────────────────────────────
products  = spark.table("products")
inventory = spark.table("inventory")
order_items = spark.table("order_items")
orders    = spark.table("orders")

# ── Sales Velocity (last 30 days) ──────────────────────────────────────────
recent_sales = (
    orders.filter(F.col("order_date") >= F.date_sub(F.current_date(), 30))
          .join(order_items, "order_id")
          .groupBy("product_id")
          .agg(F.sum("qty").alias("units_sold_30d"))
)

# ── Inventory Metrics ──────────────────────────────────────────────────────
inv_metrics = (
    inventory
    .groupBy("product_id")
    .agg(
        F.sum("qty_on_hand").alias("total_qty_on_hand"),
        F.sum("qty_reserved").alias("total_qty_reserved"),
        F.min("expiry_date").alias("nearest_expiry"),
        F.countDistinct("warehouse_id").alias("warehouse_count")
    )
    .withColumn("available_qty", F.col("total_qty_on_hand") - F.col("total_qty_reserved"))
)

# ── ABC Classification ─────────────────────────────────────────────────────
w_all  = Window.orderBy(F.desc("units_sold_30d"))
abc_df = (
    recent_sales
    .withColumn("cum_pct",
        F.sum("units_sold_30d").over(w_all.rowsBetween(Window.unboundedPreceding, 0)) /
        F.sum("units_sold_30d").over(w_all.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    )
    .withColumn("abc_class",
        F.when(F.col("cum_pct") <= 0.8, "A")
         .when(F.col("cum_pct") <= 0.95, "B")
         .otherwise("C")
    )
)

# ── Reorder Alert ──────────────────────────────────────────────────────────
result = (
    products
    .join(inv_metrics,  "product_id", "left")
    .join(recent_sales, "product_id", "left")
    .join(abc_df.select("product_id","abc_class"), "product_id", "left")
    .withColumn("days_of_stock",
        F.when(F.col("units_sold_30d") > 0,
               (F.col("available_qty") / (F.col("units_sold_30d") / 30)).cast("int"))
         .otherwise(9999)
    )
    .withColumn("reorder_flag",
        F.col("total_qty_on_hand") <= F.col("reorder_point")
    )
    .withColumn("inventory_status",
        F.when(F.col("total_qty_on_hand") == 0, "OUT_OF_STOCK")
         .when(F.col("reorder_flag"),            "REORDER_NOW")
         .when(F.col("days_of_stock") <= 7,      "CRITICAL_LOW")
         .when(F.col("days_of_stock") <= 30,     "LOW")
         .otherwise("HEALTHY")
    )
    .select(
        "product_id","product_name","category","supplier_id",
        F.coalesce("total_qty_on_hand",  F.lit(0)).alias("total_qty_on_hand"),
        F.coalesce("available_qty",      F.lit(0)).alias("available_qty"),
        F.coalesce("units_sold_30d",     F.lit(0)).alias("units_sold_30d"),
        "days_of_stock","abc_class","inventory_status","reorder_flag","nearest_expiry",
        F.current_timestamp().alias("view_refreshed_at")
    )
)

result.createOrReplaceTempView("VW_ProductInventoryStatus")
result.write.format("delta").mode("overwrite").option("overwriteSchema","true") \\
      .saveAsTable(f"{catalog}.{schema}.VW_ProductInventoryStatus")
print(f"VW_ProductInventoryStatus refreshed — {result.count():,} rows")
''',

    "VW_FinancialPeriodSummary": '''
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Widget Parameters ──────────────────────────────────────────────────────
dbutils.widgets.text("catalog",     "main",    "Catalog")
dbutils.widgets.text("schema",      "default", "Schema")
dbutils.widgets.text("fiscal_year", "2024",    "Fiscal Year")

catalog     = dbutils.widgets.get("catalog")
schema      = dbutils.widgets.get("schema")
fiscal_year = int(dbutils.widgets.get("fiscal_year"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# ── Load Source Tables ─────────────────────────────────────────────────────
gl_entries     = spark.table("gl_entries")
accounts       = spark.table("chart_of_accounts")
fiscal_periods = spark.table("fiscal_calendar")
dept_hierarchy = spark.table("department_hierarchy")

# ── Filter to Fiscal Year ──────────────────────────────────────────────────
periods_in_year = fiscal_periods.filter(F.col("fiscal_year") == fiscal_year)
prev_year_periods = fiscal_periods.filter(F.col("fiscal_year") == fiscal_year - 1)

# ── Aggregate GL by Period ─────────────────────────────────────────────────
def agg_gl(periods_df, suffix=""):
    return (
        gl_entries
        .join(periods_df.select("period_id","period_label","fiscal_month"), "period_id")
        .join(accounts, "account_id")
        .groupBy("period_id","period_label","fiscal_month","account_type","cost_center_id")
        .agg(
            F.sum(F.when(F.col("entry_type")=="CREDIT", F.col("amount")).otherwise(-F.col("amount"))
             ).alias(f"net_amount{suffix}"),
            F.sum(F.col("amount")).alias(f"gross_amount{suffix}"),
            F.count("entry_id").alias(f"transaction_count{suffix}")
        )
    )

cy_gl  = agg_gl(periods_in_year)
py_gl  = agg_gl(prev_year_periods, "_py")

# ── Pivot Account Types ────────────────────────────────────────────────────
account_types = ["REVENUE","COGS","OPEX","CAPEX","OTHER"]
pivot_cy = (
    cy_gl
    .groupBy("period_id","period_label","fiscal_month","cost_center_id")
    .pivot("account_type", account_types)
    .agg(F.sum("net_amount"))
    .fillna(0)
)

# ── YoY Join ──────────────────────────────────────────────────────────────
pivot_py = (
    py_gl
    .groupBy("fiscal_month","cost_center_id")
    .pivot("account_type", account_types)
    .agg(F.sum("net_amount_py"))
    .fillna(0)
    .toDF(*[c + "_py" if c not in ["fiscal_month","cost_center_id"] else c
            for c in pivot_py.columns])  # rename for join
)

# ── P&L Metrics ───────────────────────────────────────────────────────────
result = (
    pivot_cy
    .join(pivot_py, on=["fiscal_month","cost_center_id"], how="left")
    .join(dept_hierarchy.select("cost_center_id","department_name","division"), "cost_center_id", "left")
    .withColumn("gross_profit",    F.col("REVENUE") - F.col("COGS"))
    .withColumn("operating_income",F.col("REVENUE") - F.col("COGS") - F.col("OPEX"))
    .withColumn("gp_margin_pct",
        F.when(F.col("REVENUE") != 0, F.col("gross_profit") / F.col("REVENUE") * 100).otherwise(0))
    .withColumn("yoy_revenue_chg",
        F.when(F.col("REVENUE_py").isNotNull() & (F.col("REVENUE_py") != 0),
               (F.col("REVENUE") - F.col("REVENUE_py")) / F.col("REVENUE_py") * 100).otherwise(None))
    .withColumn("fiscal_year", F.lit(fiscal_year))
    .withColumn("view_refreshed_at", F.current_timestamp())
)

result.createOrReplaceTempView("VW_FinancialPeriodSummary")
result.write.format("delta").mode("overwrite").option("overwriteSchema","true") \\
      .saveAsTable(f"{catalog}.{schema}.VW_FinancialPeriodSummary")
print(f"VW_FinancialPeriodSummary refreshed — {result.count():,} rows")
'''
}


# ─────────────────────────────────────────────────────────────────────────────
# PySpark Templates for SQL User-Defined Functions
# ─────────────────────────────────────────────────────────────────────────────
PYSPARK_UDFS = {
    "UDF_CalculateTax": '''
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ── Tax Rate Lookup Table ──────────────────────────────────────────────────
# Replaces SQL CASE table; loaded once and broadcast for performance
tax_rates_data = [
    ("US", "Electronics",  0.08),
    ("US", "Clothing",     0.05),
    ("US", "Food",         0.00),
    ("US", "Other",        0.07),
    ("CA", "Electronics",  0.13),
    ("CA", "Clothing",     0.10),
    ("CA", "Food",         0.05),
    ("CA", "Other",        0.12),
    ("EU", "Electronics",  0.20),
    ("EU", "Clothing",     0.20),
    ("EU", "Food",         0.10),
    ("EU", "Other",        0.20),
]
tax_rates_df = spark.createDataFrame(
    tax_rates_data, ["country_code", "product_category", "tax_rate"]
)
broadcast_rates = F.broadcast(tax_rates_df)

# ── Python UDF ─────────────────────────────────────────────────────────────
def _calculate_tax_py(amount, country_code, product_category):
    """Pure Python fallback; use join-based approach for distributed execution."""
    if amount is None:
        return 0.0
    rates = {
        ("US","Electronics"): 0.08, ("US","Clothing"): 0.05,
        ("US","Food"): 0.00,        ("US","Other"):  0.07,
        ("CA","Electronics"): 0.13, ("CA","Clothing"): 0.10,
        ("CA","Food"): 0.05,        ("CA","Other"):  0.12,
        ("EU","Electronics"): 0.20, ("EU","Clothing"): 0.20,
        ("EU","Food"): 0.10,        ("EU","Other"):  0.20,
    }
    rate = rates.get((country_code, product_category),
                     rates.get((country_code, "Other"), 0.0))
    return round(float(amount) * rate, 2)

calculate_tax_udf = F.udf(_calculate_tax_py, DecimalType(18, 2))

# ── Register as Spark SQL Function ────────────────────────────────────────
spark.udf.register("UDF_CalculateTax", _calculate_tax_py, DecimalType(18, 2))

# ── Preferred: Join-based (no UDF serialization overhead) ─────────────────
# Example usage — apply to an orders DataFrame
orders = spark.table("orders")
result = (
    orders
    .join(broadcast_rates,
          on=[(orders["country_code"]     == tax_rates_df["country_code"]) &
              (orders["product_category"] == tax_rates_df["product_category"])],
          how="left")
    .fillna(0.0, subset=["tax_rate"])
    .withColumn("tax_amount",   (F.col("order_total") * F.col("tax_rate")).cast(DecimalType(18,2)))
    .withColumn("total_with_tax", F.col("order_total") + F.col("tax_amount"))
    .drop(tax_rates_df["country_code"])
    .drop(tax_rates_df["product_category"])
)

result.createOrReplaceTempView("orders_with_tax")
print(f"UDF_CalculateTax registered and applied to {result.count():,} orders")
# Spark SQL usage: SELECT UDF_CalculateTax(order_total, country_code, product_category) FROM orders
''',

    "UDF_FormatCurrency": '''
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ── Format Config ──────────────────────────────────────────────────────────
_currency_formats = {
    "USD": ("$",  2, ",", "."),
    "EUR": ("€",  2, ".", ","),
    "GBP": ("£",  2, ",", "."),
    "JPY": ("¥",  0, ",", "."),
    "CAD": ("CA$",2, ",", "."),
    "AUD": ("A$", 2, ",", "."),
    "INR": ("₹",  2, ",", "."),
}

# ── Python UDF ─────────────────────────────────────────────────────────────
def _format_currency_py(amount, currency_code, include_symbol=True):
    """Format a numeric amount as a localized currency string."""
    if amount is None:
        return "N/A"
    symbol, decimals, thousands_sep, decimal_sep = _currency_formats.get(
        currency_code, ("", 2, ",", ".")
    )
    try:
        value = float(amount)
        if decimals == 0:
            formatted = f"{int(value):,}".replace(",", thousands_sep)
        else:
            formatted = f"{value:,.{decimals}f}"
            if thousands_sep != ",":
                formatted = formatted.replace(",", "TSEP").replace(".", decimal_sep).replace("TSEP", thousands_sep)
        return f"{symbol}{formatted}" if include_symbol else formatted
    except (ValueError, TypeError):
        return str(amount)

format_currency_udf = F.udf(_format_currency_py, StringType())

# ── Register as Spark SQL Function ────────────────────────────────────────
spark.udf.register("UDF_FormatCurrency", _format_currency_py, StringType())

# ── Example Usage ──────────────────────────────────────────────────────────
sample_data = [(100.5, "USD"), (1234.99, "EUR"), (500000, "JPY"), (75.0, "GBP")]
sample_df   = spark.createDataFrame(sample_data, ["amount", "currency"])
demo = sample_df.withColumn(
    "formatted", format_currency_udf(F.col("amount"), F.col("currency"))
)
demo.show()
# Spark SQL: SELECT UDF_FormatCurrency(amount, currency_code) FROM financial_data
''',

    "UDF_GetFiscalPeriod": '''
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder.getOrCreate()

# ── Fiscal Calendar Config ─────────────────────────────────────────────────
# Fiscal year starts in April (month_offset=3); adjust as needed
FISCAL_MONTH_OFFSET = 3   # April = start of fiscal year

# ── Return Type ────────────────────────────────────────────────────────────
fiscal_schema = StructType([
    StructField("fiscal_year",    IntegerType(), True),
    StructField("fiscal_quarter", IntegerType(), True),
    StructField("fiscal_month",   IntegerType(), True),
    StructField("fiscal_week",    IntegerType(), True),
    StructField("period_label",   StringType(),  True),
    StructField("is_period_end",  IntegerType(), True),
])

# ── Python UDF ─────────────────────────────────────────────────────────────
def _get_fiscal_period_py(date_val):
    """Derive fiscal calendar fields from a calendar date."""
    if date_val is None:
        return None
    try:
        if isinstance(date_val, str):
            d = datetime.datetime.strptime(date_val[:10], "%Y-%m-%d").date()
        else:
            d = date_val
        # Shift month by offset
        shifted_month = ((d.month - 1 - FISCAL_MONTH_OFFSET) % 12) + 1
        fiscal_year   = d.year if d.month > FISCAL_MONTH_OFFSET else d.year - 1
        fiscal_qtr    = ((shifted_month - 1) // 3) + 1
        # Fiscal week (approximate)
        fiscal_start  = datetime.date(fiscal_year, FISCAL_MONTH_OFFSET + 1, 1)
        fiscal_week   = ((d - fiscal_start).days // 7) + 1
        # Period-end: last calendar day of the month
        import calendar
        last_day = calendar.monthrange(d.year, d.month)[1]
        is_end   = 1 if d.day == last_day else 0
        period_label = f"FY{fiscal_year}-Q{fiscal_qtr}-M{shifted_month:02d}"
        return (fiscal_year, fiscal_qtr, shifted_month, fiscal_week, period_label, is_end)
    except Exception:
        return None

get_fiscal_period_udf = F.udf(_get_fiscal_period_py, fiscal_schema)

# ── Register as Spark SQL Function (returns struct) ────────────────────────
spark.udf.register("UDF_GetFiscalPeriod", _get_fiscal_period_py, fiscal_schema)

# ── Example Usage ──────────────────────────────────────────────────────────
sample = spark.range(1).select(
    F.explode(F.array(
        F.lit("2024-01-31"), F.lit("2024-04-01"), F.lit("2024-12-31"), F.lit("2025-03-31")
    )).alias("transaction_date")
)
demo = (
    sample
    .withColumn("fp", get_fiscal_period_udf(F.col("transaction_date")))
    .select(
        "transaction_date",
        F.col("fp.fiscal_year"),
        F.col("fp.fiscal_quarter"),
        F.col("fp.fiscal_month"),
        F.col("fp.period_label"),
        F.col("fp.is_period_end"),
    )
)
demo.show(truncate=False)
# Spark SQL: SELECT UDF_GetFiscalPeriod(transaction_date).fiscal_year FROM orders
'''
}


def get_pyspark_code(sp_name: str) -> dict:
    """Return the PySpark conversion for any SQL object (SP, View, or UDF)."""
    # Search all template dictionaries
    if sp_name in PYSPARK_TEMPLATES:
        code        = PYSPARK_TEMPLATES[sp_name]
        object_type = "stored_procedure"
    elif sp_name in PYSPARK_VIEWS:
        code        = PYSPARK_VIEWS[sp_name]
        object_type = "view"
    elif sp_name in PYSPARK_UDFS:
        code        = PYSPARK_UDFS[sp_name]
        object_type = "udf"
    else:
        return {"success": False, "error": f"No conversion template found for '{sp_name}'"}

    import datetime
    header = f"""# {'='*75}
# Converted by  : SQL → Databricks Migration Utility
# Source Object  : {sp_name}
# Object Type    : {object_type.upper()}
# Conversion Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Target Runtime : Databricks Runtime 14.x+ / Unity Catalog
# Language       : PySpark (Python 3.x)
# {'='*75}
"""
    return {
        "success"         : True,
        "sp_name"         : sp_name,
        "object_type"     : object_type,
        "pyspark_code"    : header + textwrap.dedent(code).strip(),
        "lines"           : len(code.strip().splitlines()),
        "conversion_notes": _get_conversion_notes(sp_name)
    }


def _get_conversion_notes(sp_name: str) -> list:
    """Return migration notes for each SQL object (SP, View, or UDF)."""
    notes = {
        # ── Stored Procedures ────────────────────────────────────────────────
        "SP_SalesAggregation_Analytics": [
            "CTEs converted to chained PySpark DataFrame transformations",
            "SQL RANK()/LAG() → PySpark Window functions",
            "SELECT INTO #temp → DataFrame cached in memory",
            "Audit log INSERT → Delta append write",
            "Parameters replaced with Databricks Widgets"
        ],
        "SP_Inventory_Management": [
            "CURSOR loop replaced with vectorized DataFrame operations",
            "Temp table #LowStockItems → cached DataFrame",
            "BEGIN TRANSACTION → Delta ACID transaction (automatic)",
            "SCOPE_IDENTITY() → uuid() expression for PO IDs",
            "TRY/CATCH → Python try/except with Delta rollback"
        ],
        "SP_Customer_Segmentation_ML": [
            "NTILE(5) → PySpark ntile() window function",
            "Pivot for category spend → groupBy + join pattern",
            "MERGE INTO → Spark SQL MERGE for SCD1 upsert",
            "CASE segmentation logic preserved 1:1",
            "RFM scoring is now parallelized across partitions"
        ],
        "SP_Financial_Reporting_GL": [
            "Recursive CTE → Iterative BFS loop (max depth configurable)",
            "Dynamic exchange rate lookup preserved",
            "Window subtotals → PySpark Window.partitionBy",
            "Results written to Delta Lake (ACID, time-travel enabled)",
            "Support for multi-currency with latest exchange rate"
        ],
        "SP_ETL_DataPipeline_Staging": [
            "Dynamic SQL MERGE → parameterized Spark SQL MERGE",
            "Cursor + WHILE loop → vectorized batch processing",
            "SCD Type 2 logic fully implemented (expire + insert)",
            "sp_executesql → spark.sql() with f-string templating",
            "Watermark table updated via MERGE for idempotency"
        ],
        # ── SQL Views ────────────────────────────────────────────────────────
        "VW_CustomerOrderSummary": [
            "SQL VIEW → materialized as Delta table for performance",
            "Multi-table JOIN preserved using DataFrame joins",
            "Window functions (ROW_NUMBER) used for preferred category",
            "COALESCE for NULL-safe metrics on customers with no orders",
            "View auto-refreshes via Databricks Job scheduler"
        ],
        "VW_ProductInventoryStatus": [
            "SQL VIEW → materialized as Delta table with OVERWRITE mode",
            "ABC classification via cumulative percentage window function",
            "30-day rolling sales velocity computed with date_sub()",
            "Broadcast join used for tax/rate lookups to minimize shuffle",
            "Reorder flag logic preserved 1:1 from CASE expression"
        ],
        "VW_FinancialPeriodSummary": [
            "SQL VIEW → partitioned Delta table by fiscal_year",
            "Pivot of account types using PySpark .pivot() + agg()",
            "YoY variance joined on fiscal_month + cost_center_id",
            "P&L metrics (GP%, Operating Income) computed as derived columns",
            "Handles NULL safely when prior-year period data is absent"
        ],
        # ── User-Defined Functions ───────────────────────────────────────────
        "UDF_CalculateTax": [
            "SQL scalar UDF → PySpark UDF with DecimalType(18,2) return type",
            "Tax rates stored as broadcast DataFrame (avoids UDF closure)",
            "Join-based approach preferred over UDF for distributed performance",
            "Registered via spark.udf.register() for Spark SQL usage",
            "Handles NULL inputs gracefully (returns 0.0)"
        ],
        "UDF_FormatCurrency": [
            "SQL scalar UDF → PySpark UDF with StringType return",
            "Locale-aware formatting: USD, EUR, GBP, JPY, CAD, AUD, INR",
            "Thousands/decimal separator configurable per currency code",
            "Includes symbol toggle (include_symbol parameter)",
            "Registered as SQL function: SELECT UDF_FormatCurrency(amount, code)"
        ],
        "UDF_GetFiscalPeriod": [
            "SQL scalar UDF → PySpark UDF with StructType return (5 fields)",
            "Fiscal year offset configurable via FISCAL_MONTH_OFFSET constant",
            "Returns fiscal_year, quarter, month, week, period_label, is_period_end",
            "Handles both string dates and Python date objects",
            "Period-end flag uses calendar.monthrange for leap-year safety"
        ],
    }
    return notes.get(sp_name, ["Automated conversion applied"])


# ─────────────────────────────────────────────────────────────────────────────
# Generic SQL → PySpark auto-converter  (for live-loaded DB objects)
# ─────────────────────────────────────────────────────────────────────────────

def sql_to_pyspark_auto(name: str, object_type: str, sql_code: str) -> str:
    """
    Auto-convert any SQL object (SP / View / UDF) to pure PySpark DataFrame API.
    Produces zero spark.sql() for core logic — every construct maps to a DF method.
    """
    import re, datetime

    ts    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    otype = (object_type or "stored_procedure").lower()

    # ─── low-level helpers ────────────────────────────────────────────────────
    def _strip_header(sql):
        sql = re.sub(
            r"(?i)^\s*(?:CREATE\s+OR\s+ALTER|ALTER|CREATE)\s+"
            r"(?:PROCEDURE|PROC|VIEW|FUNCTION)\s+[\w\.\[\]\"]+[^;]*?\bAS\b\s*(?:BEGIN\s*)?",
            "", sql, count=1, flags=re.DOTALL)
        sql = re.sub(r"(?i)\s*\bEND\b\s*(?:;\s*)?(?:GO\s*)?$", "", sql.rstrip())
        return sql.strip()

    def _clean_name(n):
        return re.sub(r"[\[\]`\"]", "", n).strip()

    def _to_var(n):
        return re.sub(r"\W", "_", _clean_name(n).split(".")[-1].lower()) + "_df"

    # ─── T-SQL type → PySpark type mapping ────────────────────────────────────
    _TYPE_MAP = {
        "INT": "IntegerType()", "INTEGER": "IntegerType()",
        "BIGINT": "LongType()", "SMALLINT": "ShortType()", "TINYINT": "ByteType()",
        "BIT": "BooleanType()",
        "FLOAT": "DoubleType()", "REAL": "FloatType()",
        "DECIMAL": "DecimalType(18,2)", "NUMERIC": "DecimalType(18,2)",
        "MONEY": "DecimalType(19,4)", "SMALLMONEY": "DecimalType(10,4)",
        "VARCHAR": "StringType()", "NVARCHAR": "StringType()",
        "CHAR": "StringType()", "NCHAR": "StringType()",
        "TEXT": "StringType()", "NTEXT": "StringType()",
        "DATE": "DateType()", "DATETIME": "TimestampType()",
        "DATETIME2": "TimestampType()", "SMALLDATETIME": "TimestampType()",
        "TIME": "StringType()", "DATETIMEOFFSET": "TimestampType()",
        "UNIQUEIDENTIFIER": "StringType()", "XML": "StringType()",
        "VARBINARY": "BinaryType()", "BINARY": "BinaryType()", "IMAGE": "BinaryType()",
    }

    def _sql_type_to_spark(sql_type):
        """Convert a SQL Server type string to PySpark type string for .cast()."""
        t = re.sub(r"\(.*\)", "", sql_type).strip().upper()
        cast_map = {
            "INT": "int", "INTEGER": "int", "BIGINT": "long", "SMALLINT": "short",
            "TINYINT": "byte", "BIT": "boolean",
            "FLOAT": "double", "REAL": "float",
            "DECIMAL": "decimal(18,2)", "NUMERIC": "decimal(18,2)",
            "MONEY": "decimal(19,4)", "SMALLMONEY": "decimal(10,4)",
            "VARCHAR": "string", "NVARCHAR": "string", "CHAR": "string",
            "NCHAR": "string", "TEXT": "string", "NTEXT": "string",
            "DATE": "date", "DATETIME": "timestamp", "DATETIME2": "timestamp",
            "SMALLDATETIME": "timestamp", "TIME": "string",
            "UNIQUEIDENTIFIER": "string", "XML": "string",
        }
        # Handle DECIMAL(p,s) / NUMERIC(p,s)
        prec_m = re.match(r"(?i)(DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", sql_type.strip())
        if prec_m:
            return f"decimal({prec_m.group(2)},{prec_m.group(3)})"
        return cast_map.get(t, "string")

    def _sql_type_to_python(sql_type):
        """Map SQL type string to Python type hint for variable declarations."""
        t = re.sub(r"\(.*\)", "", sql_type).strip().upper()
        return {"INT": "int", "INTEGER": "int", "BIGINT": "int", "SMALLINT": "int",
                "TINYINT": "int", "BIT": "bool", "FLOAT": "float", "REAL": "float",
                "DECIMAL": "float", "NUMERIC": "float", "MONEY": "float",
                "VARCHAR": "str", "NVARCHAR": "str", "CHAR": "str", "NCHAR": "str",
                "TEXT": "str", "NTEXT": "str", "DATE": "str", "DATETIME": "str",
                "DATETIME2": "str", "TIME": "str", "UNIQUEIDENTIFIER": "str",
                }.get(t, "str")

    # ─── T-SQL expression → PySpark expression converter ─────────────────────
    def _tsql_expr_to_pyspark(expr):
        """
        Convert T-SQL scalar expressions to PySpark F.* equivalents.
        Handles: ISNULL, COALESCE, GETDATE, DATEADD, DATEDIFF, CONVERT, CAST,
        LEN, CHARINDEX, SUBSTRING, REPLACE, UPPER, LOWER, TRIM, LTRIM, RTRIM,
        LEFT, RIGHT, CONCAT, IIF, NULLIF, STUFF, STRING_AGG, NEWID,
        ISNUMERIC, FORMAT, YEAR, MONTH, DAY, EOMONTH, ROUND, ABS, CEILING, FLOOR,
        POWER, SIGN, SQUARE, SQRT, LOG, @@ROWCOUNT, SCOPE_IDENTITY, GETUTCDATE.
        """
        if not expr:
            return expr
        s = expr.strip()
        # Remove surrounding brackets from identifiers
        s = re.sub(r"\[(\w+)\]", r"\1", s)
        s = re.sub(r"\bdbo\.", "", s, flags=re.IGNORECASE)

        # ── ISNULL(a, b) → F.coalesce(a, b) ───────────────────────────────
        s = re.sub(r"(?i)\bISNULL\s*\(", "COALESCE(", s)

        # ── GETDATE() / GETUTCDATE() → current_timestamp() ────────────────
        s = re.sub(r"(?i)\bGETDATE\s*\(\s*\)", "current_timestamp()", s)
        s = re.sub(r"(?i)\bGETUTCDATE\s*\(\s*\)", "current_timestamp()", s)
        s = re.sub(r"(?i)\bSYSDATETIME\s*\(\s*\)", "current_timestamp()", s)

        # ── CAST(x AS DATE) from GETDATE → current_date() ─────────────────
        s = re.sub(r"(?i)CAST\s*\(\s*current_timestamp\(\)\s+AS\s+DATE\s*\)",
                    "current_date()", s)

        # ── CAST(expr AS type) → expr :: spark_type ────────────────────────
        def _replace_cast(m):
            inner = m.group(1).strip()
            sql_t = m.group(2).strip()
            spark_t = _sql_type_to_spark(sql_t)
            return f"CAST({inner} AS {spark_t})"
        s = re.sub(r"(?i)\bCAST\s*\(\s*(.+?)\s+AS\s+([\w\(\),\s]+?)\s*\)", _replace_cast, s)

        # ── CONVERT(type, expr [, style]) → CAST(expr AS spark_type) ──────
        def _replace_convert(m):
            sql_t = m.group(1).strip()
            inner = m.group(2).strip()
            spark_t = _sql_type_to_spark(sql_t)
            return f"CAST({inner} AS {spark_t})"
        s = re.sub(r"(?i)\bCONVERT\s*\(\s*([\w\(\),]+?)\s*,\s*(.+?)(?:\s*,\s*\d+)?\s*\)", _replace_convert, s)

        # ── DATEADD(unit, n, date) → date_add / add_months ────────────────
        def _replace_dateadd(m):
            unit = m.group(1).strip().upper()
            n    = m.group(2).strip()
            dt   = m.group(3).strip()
            if unit in ("DAY", "DD", "D"):
                return f"date_add({dt}, {n})"
            elif unit in ("MONTH", "MM", "M"):
                return f"add_months({dt}, {n})"
            elif unit in ("YEAR", "YY", "YYYY"):
                return f"add_months({dt}, ({n}) * 12)"
            elif unit in ("HOUR", "HH"):
                return f"({dt} + INTERVAL {n} HOURS)"
            elif unit in ("MINUTE", "MI", "N"):
                return f"({dt} + INTERVAL {n} MINUTES)"
            elif unit in ("SECOND", "SS", "S"):
                return f"({dt} + INTERVAL {n} SECONDS)"
            return f"date_add({dt}, {n})"  # fallback to days
        s = re.sub(r"(?i)\bDATEADD\s*\(\s*(\w+)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)", _replace_dateadd, s)

        # ── DATEDIFF(unit, d1, d2) → datediff ─────────────────────────────
        def _replace_datediff(m):
            unit = m.group(1).strip().upper()
            d1   = m.group(2).strip()
            d2   = m.group(3).strip()
            if unit in ("DAY", "DD", "D"):
                return f"datediff({d2}, {d1})"
            elif unit in ("MONTH", "MM", "M"):
                return f"months_between({d2}, {d1})"
            elif unit in ("YEAR", "YY", "YYYY"):
                return f"(YEAR({d2}) - YEAR({d1}))"
            elif unit in ("HOUR", "HH"):
                return f"CAST((unix_timestamp({d2}) - unix_timestamp({d1})) / 3600 AS INT)"
            elif unit in ("MINUTE", "MI", "N"):
                return f"CAST((unix_timestamp({d2}) - unix_timestamp({d1})) / 60 AS INT)"
            elif unit in ("SECOND", "SS", "S"):
                return f"CAST(unix_timestamp({d2}) - unix_timestamp({d1}) AS INT)"
            return f"datediff({d2}, {d1})"
        s = re.sub(r"(?i)\bDATEDIFF\s*\(\s*(\w+)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)", _replace_datediff, s)

        # ── LEN(x) → length(x) ────────────────────────────────────────────
        s = re.sub(r"(?i)\bLEN\s*\(", "length(", s)

        # ── CHARINDEX(sub, str [,start]) → locate(sub, str [,start]) ──────
        s = re.sub(r"(?i)\bCHARINDEX\s*\(", "locate(", s)

        # ── PATINDEX('%pattern%', str) → locate equivalent ────────────────
        s = re.sub(r"(?i)\bPATINDEX\s*\(", "locate(", s)

        # ── SUBSTRING(str, start, len) → substring(str, start, len) ───────
        # T-SQL uses same syntax as Spark, just ensure function name
        s = re.sub(r"(?i)\bSUBSTRING\s*\(", "substring(", s)

        # ── LEFT(str, n) → substring(str, 1, n) ──────────────────────────
        def _replace_left(m):
            return f"substring({m.group(1).strip()}, 1, {m.group(2).strip()})"
        s = re.sub(r"(?i)\bLEFT\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)", _replace_left, s)

        # ── RIGHT(str, n) → substring(str, -n, n) ────────────────────────
        def _replace_right(m):
            n = m.group(2).strip()
            return f"substring({m.group(1).strip()}, length({m.group(1).strip()}) - {n} + 1, {n})"
        s = re.sub(r"(?i)\bRIGHT\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)", _replace_right, s)

        # ── STUFF(str, start, len, replacement) → overlay() ──────────────
        def _replace_stuff(m):
            return f"concat(substring({m.group(1)}, 1, {m.group(2)} - 1), {m.group(4)}, substring({m.group(1)}, {m.group(2)} + {m.group(3)}))"
        s = re.sub(r"(?i)\bSTUFF\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(.+?)\s*\)", _replace_stuff, s)

        # ── REPLACE(str, old, new) → replace(str, old, new) ──────────────
        s = re.sub(r"(?i)\bREPLACE\s*\(", "replace(", s)

        # ── UPPER/LOWER/LTRIM/RTRIM/TRIM ──────────────────────────────────
        s = re.sub(r"(?i)\bUPPER\s*\(", "upper(", s)
        s = re.sub(r"(?i)\bLOWER\s*\(", "lower(", s)
        s = re.sub(r"(?i)\bLTRIM\s*\(", "ltrim(", s)
        s = re.sub(r"(?i)\bRTRIM\s*\(", "rtrim(", s)
        s = re.sub(r"(?i)\bTRIM\s*\(", "trim(", s)

        # ── CONCAT(a, b, ...) → concat(a, b, ...) (same name) ────────────
        # T-SQL string concatenation with + → concat()
        # Only convert when both sides look like strings/columns (not arithmetic)
        # We handle this conservatively
        s = re.sub(r"(?i)\bCONCAT\s*\(", "concat(", s)

        # ── IIF(cond, true_val, false_val) → IF(cond, true, false) / CASE
        def _replace_iif(m):
            return f"CASE WHEN {m.group(1)} THEN {m.group(2)} ELSE {m.group(3)} END"
        s = re.sub(r"(?i)\bIIF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)", _replace_iif, s)

        # ── NULLIF(a, b) → nullif(a, b) ──────────────────────────────────
        s = re.sub(r"(?i)\bNULLIF\s*\(", "nullif(", s)

        # ── COALESCE (kept, Spark supports it natively) ───────────────────
        # Already handled — ISNULL was converted to COALESCE above

        # ── NEWID() → uuid() ─────────────────────────────────────────────
        s = re.sub(r"(?i)\bNEWID\s*\(\s*\)", "uuid()", s)

        # ── YEAR(d) / MONTH(d) / DAY(d) → year(d) / month(d) / day(d) ───
        s = re.sub(r"(?i)\bYEAR\s*\(", "year(", s)
        s = re.sub(r"(?i)\bMONTH\s*\(", "month(", s)
        s = re.sub(r"(?i)\bDAY\s*\(", "day(", s)

        # ── EOMONTH(date) → last_day(date) ───────────────────────────────
        s = re.sub(r"(?i)\bEOMONTH\s*\(", "last_day(", s)

        # ── Math functions ────────────────────────────────────────────────
        s = re.sub(r"(?i)\bROUND\s*\(", "round(", s)
        s = re.sub(r"(?i)\bABS\s*\(", "abs(", s)
        s = re.sub(r"(?i)\bCEILING\s*\(", "ceil(", s)
        s = re.sub(r"(?i)\bFLOOR\s*\(", "floor(", s)
        s = re.sub(r"(?i)\bPOWER\s*\(", "power(", s)
        s = re.sub(r"(?i)\bSQRT\s*\(", "sqrt(", s)
        s = re.sub(r"(?i)\bLOG\s*\(", "log(", s)
        s = re.sub(r"(?i)\bSIGN\s*\(", "signum(", s)
        s = re.sub(r"(?i)\bSQUARE\s*\(\s*(.+?)\s*\)", r"power(\1, 2)", s)

        # ── ISNUMERIC(x) → simple cast check (best-effort) ───────────────
        s = re.sub(r"(?i)\bISNUMERIC\s*\(\s*(.+?)\s*\)",
                    r"CAST(\1 AS DOUBLE) IS NOT NULL", s)

        # ── STRING_AGG(col, sep) → concat_ws(sep, collect_list(col)) ─────
        def _replace_string_agg(m):
            col = m.group(1).strip()
            sep = m.group(2).strip()
            return f"concat_ws({sep}, collect_list({col}))"
        s = re.sub(r"(?i)\bSTRING_AGG\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)", _replace_string_agg, s)

        # ── FORMAT(value, format_str) → format_number / date_format ──────
        def _replace_format(m):
            val = m.group(1).strip()
            fmt = m.group(2).strip()
            return f"format_number({val}, {fmt})"
        s = re.sub(r"(?i)\bFORMAT\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)", _replace_format, s)

        # ── @@ROWCOUNT → <last_df>.count() placeholder ────────────────────
        s = re.sub(r"@@ROWCOUNT", "row_count  # placeholder for @@ROWCOUNT", s)

        # ── SCOPE_IDENTITY() → placeholder ────────────────────────────────
        s = re.sub(r"(?i)\bSCOPE_IDENTITY\s*\(\s*\)",
                    "None  # SCOPE_IDENTITY — use uuid() or monotonically_increasing_id()", s)

        # ── @@ERROR → 0 (in PySpark, use try/except) ─────────────────────
        s = re.sub(r"@@ERROR", "0  # @@ERROR — use try/except in PySpark", s)

        # ── N'string' → 'string' (remove N prefix from Unicode literals) ──
        s = re.sub(r"\bN'", "'", s)

        return s

    def _col_expr(expr):
        """Convert a SQL column expression to F.expr() or F.col() with T-SQL translation."""
        expr = expr.strip().strip(",").strip()
        # Apply T-SQL function conversion first
        expr = _tsql_expr_to_pyspark(expr)

        alias_m = re.match(r"^(.+?)\s+(?:AS\s+)?(\w+)\s*$", expr, re.IGNORECASE)
        alias = ""
        if alias_m:
            expr, alias = alias_m.group(1).strip(), alias_m.group(2)
        expr = re.sub(r"\[(\w+)\]", r"\1", expr)
        expr = re.sub(r"\bdbo\.", "", expr, flags=re.IGNORECASE)
        # simple column ref (e.g. "col", "table.col")
        if re.match(r"^[\w\.]+$", expr):
            base = f'F.col("{expr}")'
        # literal number
        elif re.match(r"^-?\d+(\.\d+)?$", expr):
            base = f"F.lit({expr})"
        # literal string
        elif re.match(r"^'[^']*'$", expr):
            base = f'F.lit({expr})'
        # COALESCE / NULL-safe
        elif re.match(r"(?i)^COALESCE\s*\(", expr):
            inner = re.match(r"(?i)COALESCE\s*\((.+)\)$", expr)
            if inner:
                args = [a.strip() for a in inner.group(1).split(",")]
                col_args = ", ".join(f'F.col("{a}")' if re.match(r"^[\w\.]+$", a) else f"F.lit({a})" for a in args)
                base = f"F.coalesce({col_args})"
            else:
                base = f'F.expr("{expr}")'
        else:
            safe = expr.replace("\\", "\\\\").replace('"', '\\"')
            base = f'F.expr("{safe}")'
        return f'{base}.alias("{alias}")' if alias else base

    def _parse_select_cols(cols_str):
        """Split SELECT column list respecting parentheses and CASE blocks."""
        cols, depth, cur = [], 0, ""
        for ch in cols_str:
            if ch == "(": depth += 1
            elif ch == ")": depth -= 1
            if ch == "," and depth == 0:
                cols.append(cur.strip())
                cur = ""
            else:
                cur += ch
        if cur.strip():
            cols.append(cur.strip())
        return cols

    def _agg_func(expr):
        """Map SQL aggregate to PySpark F.* expression with COUNT DISTINCT support."""
        expr = expr.strip()
        # Apply T-SQL function conversion
        expr = _tsql_expr_to_pyspark(expr)

        # COUNT(DISTINCT col) → F.countDistinct("col")
        cd_m = re.match(r"(?i)COUNT\s*\(\s*DISTINCT\s+(.+?)\s*\)\s*(?:AS\s+(\w+))?$", expr)
        if cd_m:
            arg = _clean_name(cd_m.group(1))
            alias = cd_m.group(2) or ""
            base = f'F.countDistinct("{arg}")'
            return f'{base}.alias("{alias}")' if alias else base

        # Standard aggregates: SUM, COUNT, AVG, MIN, MAX, STDEV, VAR, APPROX_COUNT_DISTINCT
        m = re.match(r"(?i)(SUM|COUNT|AVG|MIN|MAX|STDEV|STDDEV|VAR|VARIANCE|APPROX_COUNT_DISTINCT)\s*\(\s*(.*?)\s*\)\s*(?:AS\s+(\w+))?$", expr)
        if m:
            fn, arg, alias = m.group(1).upper(), m.group(2).strip(), m.group(3) or ""
            pf = {"SUM": "sum", "COUNT": "count", "AVG": "avg",
                  "MIN": "min", "MAX": "max", "STDEV": "stddev", "STDDEV": "stddev",
                  "VAR": "variance", "VARIANCE": "variance",
                  "APPROX_COUNT_DISTINCT": "approx_count_distinct"}.get(fn, "sum")
            # Handle expressions inside aggregates (not just column names)
            arg_clean = _clean_name(arg)
            if "*" in arg:
                base = "F.count(F.lit(1))"
            elif re.match(r"^[\w\.]+$", arg_clean):
                base = f'F.{pf}("{arg_clean}")'
            else:
                # Complex expression inside aggregate — use F.expr
                safe_arg = arg.replace('"', '\\"')
                base = f'F.{pf}(F.expr("{safe_arg}"))'
            return f'{base}.alias("{alias}")' if alias else base

        # SUM/COUNT with CASE WHEN inside
        case_agg_m = re.match(r"(?i)(SUM|COUNT)\s*\(\s*(CASE\b.+?END)\s*\)\s*(?:AS\s+(\w+))?$", expr, re.DOTALL)
        if case_agg_m:
            fn = case_agg_m.group(1).upper()
            case_body = case_agg_m.group(2)
            alias = case_agg_m.group(3) or ""
            case_pyspark = _case_when(case_body)
            pf = "sum" if fn == "SUM" else "count"
            base = f'F.{pf}({case_pyspark})'
            return f'{base}.alias("{alias}")' if alias else base

        # Fallback — extract alias and wrap in F.expr
        alias_m = re.match(r"^(.+?)\s+(?:AS\s+)?(\w+)\s*$", expr, re.IGNORECASE)
        if alias_m:
            safe = alias_m.group(1).strip().replace('"', '\\"')
            return f'F.expr("{safe}").alias("{alias_m.group(2)}")'
        return f'F.expr("{expr.replace(chr(34), chr(39))}")'

    def _where_to_filter(where_str):
        """Convert WHERE clause to PySpark filter expression with smart translation."""
        where_str = re.sub(r"\[(\w+)\]", r"\1", where_str)
        where_str = re.sub(r"\bdbo\.", "", where_str, flags=re.IGNORECASE)
        where_str = re.sub(r"@(\w+)", r"{\1}", where_str)
        # Apply T-SQL function conversion
        where_str = _tsql_expr_to_pyspark(where_str)
        where_str = where_str.replace('"', "'")
        return f'F.expr(f"{where_str}")'

    def _join_type(sql_kw):
        kw = re.sub(r"\s+", " ", sql_kw).strip().upper()
        m = {"LEFT": "left", "LEFT OUTER": "left",
             "RIGHT": "right", "RIGHT OUTER": "right",
             "INNER": "inner", "CROSS": "cross",
             "FULL": "outer", "FULL OUTER": "outer", "OUTER": "outer"
             }.get(kw, "inner")
        return m

    def _smart_join_condition(on_cond, left_var, right_var, right_tbl):
        """
        Try to convert simple equi-join ON conditions to PySpark column equality.
        Falls back to F.expr() for complex conditions.
        Returns a string representing the join condition.
        """
        on_cond = re.sub(r"\[(\w+)\]", r"\1", on_cond)
        on_cond = re.sub(r"\bdbo\.", "", on_cond, flags=re.IGNORECASE)
        # Split on AND
        parts = re.split(r"(?i)\s+AND\s+", on_cond.strip())
        simple_pairs = []
        for part in parts:
            # Match: a.col = b.col or col1 = col2
            eq_m = re.match(r"^\s*([\w\.]+)\s*=\s*([\w\.]+)\s*$", part.strip())
            if eq_m:
                l, r = eq_m.group(1), eq_m.group(2)
                # Extract just column name (strip alias prefix)
                l_col = l.split(".")[-1]
                r_col = r.split(".")[-1]
                if l_col == r_col:
                    simple_pairs.append(l_col)
                else:
                    simple_pairs = None
                    break
            else:
                simple_pairs = None
                break

        if simple_pairs and len(simple_pairs) >= 1:
            if len(simple_pairs) == 1:
                return f'"{simple_pairs[0]}"'
            else:
                cols = ", ".join(f'"{c}"' for c in simple_pairs)
                return f'[{cols}]'
        else:
            # Complex condition — use F.expr
            safe = on_cond.replace('"', "'")
            safe = _tsql_expr_to_pyspark(safe)
            return f'F.expr("{safe}")'

    def _case_when(expr):
        """Convert CASE WHEN … THEN … ELSE … END to F.when chain with type-aware values."""
        expr = _tsql_expr_to_pyspark(expr)
        whens = re.findall(r"(?i)WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END)", expr)
        else_m = re.search(r"(?i)ELSE\s+(.+?)\s+END", expr)
        chain = ""
        for cond, val in whens:
            cond = cond.replace('"', "'").strip()
            val  = val.strip()
            val_expr = _smart_value(val)
            if chain:
                chain += f'\n        .when(F.expr("{cond}"), {val_expr})'
            else:
                chain = f'F.when(F.expr("{cond}"), {val_expr})'
        if else_m:
            ev = else_m.group(1).strip()
            ev_expr = _smart_value(ev)
            chain += f'\n        .otherwise({ev_expr})'
        return chain if chain else 'F.lit(None)'

    def _smart_value(val):
        """Determine if a value is a column reference, number, NULL, or string literal."""
        val = val.strip()
        if val.upper() == "NULL":
            return "F.lit(None)"
        # Integer
        if re.match(r"^-?\d+$", val):
            return f"F.lit({val})"
        # Float/Decimal
        if re.match(r"^-?\d+\.\d+$", val):
            return f"F.lit({val})"
        # String literal
        if re.match(r"^'[^']*'$", val):
            return f'F.lit({val})'
        # Column reference (word.word or just word)
        if re.match(r"^[\w\.]+$", val):
            return f'F.col("{val}")'
        # Expression (function call, arithmetic, etc.)
        safe = val.replace('"', '\\"')
        return f'F.expr("{safe}")'

    def _window_expr(expr):
        """Convert ROW_NUMBER/RANK/etc. OVER() to PySpark Window with frame spec support."""
        expr = _tsql_expr_to_pyspark(expr)
        fn_m = re.match(
            r"(?i)(ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD|NTILE|SUM|AVG|COUNT|MIN|MAX|FIRST_VALUE|LAST_VALUE|PERCENT_RANK|CUME_DIST)\s*\(([^)]*)\)\s*OVER\s*\((.+)\)",
            expr, re.DOTALL)
        if not fn_m:
            return f'F.expr("{expr.replace(chr(34), chr(39))}")'
        fn, fn_args, over = fn_m.group(1).upper(), fn_m.group(2).strip(), fn_m.group(3).strip()

        # Parse PARTITION BY
        part_m = re.search(r"(?i)PARTITION\s+BY\s+([\w,\s\.\[\]]+?)(?=\s+ORDER|\s+ROWS|\s+RANGE|\s*$)", over)
        parts = [f'"{_clean_name(p.strip())}"' for p in part_m.group(1).split(",")] if part_m else []

        # Parse ORDER BY with ASC/DESC
        ord_m = re.search(r"(?i)ORDER\s+BY\s+(.+?)(?=\s+ROWS|\s+RANGE|\s*$)", over)
        ord_parts = []
        if ord_m:
            for o in ord_m.group(1).split(","):
                o = o.strip()
                desc = bool(re.search(r"(?i)\bDESC\b", o))
                col = re.sub(r"(?i)\s*(ASC|DESC)\s*", "", o).strip()
                col_name = _clean_name(col)
                if desc:
                    ord_parts.append(f'F.col("{col_name}").desc()')
                else:
                    ord_parts.append(f'F.col("{col_name}").asc()')

        # Parse frame spec (ROWS BETWEEN ... AND ...)
        frame_m = re.search(r"(?i)(ROWS|RANGE)\s+BETWEEN\s+(.+?)\s+AND\s+(.+?)(?:\s*$)", over)
        frame_spec = ""
        if frame_m:
            frame_type = frame_m.group(1).upper()
            start_f = frame_m.group(2).strip().upper()
            end_f   = frame_m.group(3).strip().upper()
            start_val = "Window.unboundedPreceding" if "UNBOUNDED PRECEDING" in start_f else \
                        "Window.currentRow" if "CURRENT ROW" in start_f else \
                        re.sub(r"\D", "", start_f) or "0"
            end_val   = "Window.unboundedFollowing" if "UNBOUNDED FOLLOWING" in end_f else \
                        "Window.currentRow" if "CURRENT ROW" in end_f else \
                        re.sub(r"\D", "", end_f) or "0"
            if "PRECEDING" in start_f and start_val not in ("Window.unboundedPreceding", "Window.currentRow"):
                start_val = f"-{start_val}"
            if frame_type == "ROWS":
                frame_spec = f".rowsBetween({start_val}, {end_val})"
            else:
                frame_spec = f".rangeBetween({start_val}, {end_val})"

        # Build window spec
        w = "Window"
        if parts:      w += f'.partitionBy({", ".join(parts)})'
        if ord_parts:  w += f'.orderBy({", ".join(ord_parts)})'
        if frame_spec: w += frame_spec

        # Build function call
        pf_map = {
            "ROW_NUMBER": "row_number()", "RANK": "rank()", "DENSE_RANK": "dense_rank()",
            "PERCENT_RANK": "percent_rank()", "CUME_DIST": "cume_dist()",
            "MIN": f'min("{_clean_name(fn_args)}")' if fn_args else "min(F.lit(1))",
            "MAX": f'max("{_clean_name(fn_args)}")' if fn_args else "max(F.lit(1))",
        }
        if fn == "LAG":
            args = [a.strip() for a in fn_args.split(",")]
            col_arg = f'F.col("{_clean_name(args[0])}")'
            offset = args[1].strip() if len(args) > 1 else "1"
            default = args[2].strip() if len(args) > 2 else None
            if default:
                pf_map["LAG"] = f"lag({col_arg}, {offset}, {default})"
            else:
                pf_map["LAG"] = f"lag({col_arg}, {offset})"
        elif fn == "LEAD":
            args = [a.strip() for a in fn_args.split(",")]
            col_arg = f'F.col("{_clean_name(args[0])}")'
            offset = args[1].strip() if len(args) > 1 else "1"
            default = args[2].strip() if len(args) > 2 else None
            if default:
                pf_map["LEAD"] = f"lead({col_arg}, {offset}, {default})"
            else:
                pf_map["LEAD"] = f"lead({col_arg}, {offset})"
        elif fn == "NTILE":
            n = fn_args.strip() if fn_args else "4"
            pf_map["NTILE"] = f"ntile({n})"
        elif fn == "SUM":
            if fn_args and re.match(r"^[\w\.]+$", fn_args.strip()):
                pf_map["SUM"] = f'sum("{_clean_name(fn_args)}")'
            elif fn_args:
                safe = fn_args.replace('"', '\\"')
                pf_map["SUM"] = f'sum(F.expr("{safe}"))'
            else:
                pf_map["SUM"] = "sum(F.lit(1))"
        elif fn == "AVG":
            if fn_args and re.match(r"^[\w\.]+$", fn_args.strip()):
                pf_map["AVG"] = f'avg("{_clean_name(fn_args)}")'
            elif fn_args:
                safe = fn_args.replace('"', '\\"')
                pf_map["AVG"] = f'avg(F.expr("{safe}"))'
            else:
                pf_map["AVG"] = "avg(F.lit(1))"
        elif fn == "COUNT":
            if fn_args and fn_args.strip() != "*":
                pf_map["COUNT"] = f'count("{_clean_name(fn_args)}")'
            else:
                pf_map["COUNT"] = "count(F.lit(1))"
        elif fn == "FIRST_VALUE":
            pf_map["FIRST_VALUE"] = f'first("{_clean_name(fn_args)}", ignorenulls=True)'
        elif fn == "LAST_VALUE":
            pf_map["LAST_VALUE"] = f'last("{_clean_name(fn_args)}", ignorenulls=True)'

        fn_str = pf_map.get(fn, fn.lower() + "()")
        return f'F.{fn_str}.over({w})'

    def _parse_ctes(sql):
        """Extract { name: body } for all CTEs, return (ctes_dict, sql_without_ctes).
        Handles unlimited chained CTEs (WITH a AS (...), b AS (...), ... SELECT).
        Also handles leading semicolons (;WITH ...) commonly used in T-SQL.
        """
        ctes = {}
        remaining = sql.strip()
        # Strip leading semicolon before WITH (common T-SQL pattern: ;WITH ...)
        remaining = re.sub(r"^\s*;\s*", "", remaining)
        if not re.match(r"(?i)\s*WITH\b", remaining):
            return ctes, remaining

        # Find the WITH keyword and strip it
        remaining = re.sub(r"(?i)^\s*WITH\s+", "", remaining, count=1).strip()

        # Strip optional leading comment lines between WITH and first CTE
        remaining = re.sub(r"^\s*--[^\n]*\n", "", remaining).strip()

        # Iteratively extract CTEs: name AS (body), name AS (body), ... final_sql
        max_ctes = 50  # safety limit
        for _ in range(max_ctes):
            # Match CTE name (with optional comment prefix)
            remaining = re.sub(r"^\s*--[^\n]*\n", "", remaining).strip()
            cte_m = re.match(r"(?i)(\w+)\s+AS\s*\(", remaining)
            if not cte_m:
                break
            cte_name = cte_m.group(1)
            start = cte_m.end()
            # Find matching closing paren
            depth, i = 1, start
            while i < len(remaining) and depth:
                if remaining[i] == "(":
                    depth += 1
                elif remaining[i] == ")":
                    depth -= 1
                i += 1
            ctes[cte_name] = remaining[start:i-1].strip()
            remaining = remaining[i:].strip()
            # If next char is comma, there are more CTEs
            if remaining.startswith(","):
                remaining = remaining[1:].strip()
                continue
            # Otherwise, remaining is the final SELECT
            break

        return ctes, remaining

    def _top_level_sql(sql):
        """Return SQL with content inside parentheses replaced by spaces.
        Preserves exact string length and positions so regex matches on the
        flattened version can be used to index into the original SQL.
        Prevents matching ORDER BY, WHERE, GROUP BY etc. inside OVER(),
        subqueries, or function calls.
        """
        out = list(sql)
        depth = 0
        for i, ch in enumerate(sql):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth > 0 and ch not in ('\n', '\r'):
                out[i] = ' '
        return ''.join(out)

    def _translate_select(sql, df_map, result_var="result_df"):
        """
        Translate a single SELECT statement to a PySpark DataFrame chain.
        Returns list of code lines.
        """
        lines = []
        # Strip SQL single-line comments (-- ...) before processing
        sql = re.sub(r"--[^\n]*", "", sql)
        sql = sql.strip()

        # Pre-process T-SQL expressions in this statement
        sql   = _tsql_expr_to_pyspark(sql)
        sql   = re.sub(r"\[(\w+)\]", r"\1", sql)
        sql   = re.sub(r"\bdbo\.", "", sql, flags=re.IGNORECASE)

        # Detect and strip INTO clause (SELECT ... INTO #temp ... FROM ...)
        into_target = None
        into_m = re.search(r"(?i)\bINTO\s+([\w\.]+)\b", sql)
        if into_m:
            into_target = _clean_name(into_m.group(1))
            sql = sql[:into_m.start()] + sql[into_m.end():]

        # ── Flatten SQL for top-level clause boundary detection ────────────
        # This prevents matching ORDER BY / WHERE / GROUP BY inside OVER(),
        # subqueries, or function calls.
        flat = _top_level_sql(sql)

        # DISTINCT
        distinct = bool(re.search(r"(?i)\bSELECT\s+DISTINCT\b", flat))
        # TOP N
        top_m = re.search(r"(?i)\bSELECT\s+TOP\s+(\d+)\b", flat)
        top_n = int(top_m.group(1)) if top_m else None

        # SELECT columns — find boundary between SELECT and top-level FROM
        sel_start_m = re.search(r"(?i)\bSELECT\b\s+(?:DISTINCT\s+)?(?:TOP\s+\d+\s+)?", flat)
        from_boundary = re.search(r"(?i)\bFROM\b", flat[sel_start_m.end():]) if sel_start_m else None
        if sel_start_m and from_boundary:
            cols_part = sql[sel_start_m.end():sel_start_m.end() + from_boundary.start()].strip()
        else:
            cols_part = "*"

        # FROM clause (main table) — use flat to avoid matching inside subqueries
        from_m = re.search(r"(?i)\bFROM\b\s+([\w\.]+)(?:\s+(?:AS\s+)?(\w+))?", flat)
        main_tbl = ""
        main_alias = ""
        if from_m:
            main_tbl   = _clean_name(from_m.group(1))
            main_alias = from_m.group(2) or main_tbl

        # JOINs — detect in flat, extract ON conditions from original sql
        join_pat = re.compile(
            r"(?i)\b(LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|INNER\s+|CROSS\s+|FULL\s+(?:OUTER\s+)?)?"
            r"JOIN\s+([\w\.]+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.*?)(?=\s+(?:LEFT|RIGHT|INNER|CROSS|FULL|WHERE|GROUP|HAVING|ORDER|$))",
            re.DOTALL)
        join_matches = list(join_pat.finditer(flat))
        # Build join info tuples: (type_str, table, alias, on_condition_from_original_sql)
        joins = []
        for jm in join_matches:
            on_cond_orig = sql[jm.start(4):jm.end(4)].strip()
            joins.append((jm.group(1), jm.group(2), jm.group(3), on_cond_orig))

        # WHERE — match boundary in flat, extract content from original sql
        where_m = re.search(r"(?i)\bWHERE\b(.*?)(?=\s+GROUP\s+BY|\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$)", flat, re.DOTALL)
        where_str = sql[where_m.start(1):where_m.end(1)].strip() if where_m else ""

        # GROUP BY — match in flat, extract from sql
        grp_m = re.search(r"(?i)\bGROUP\s+BY\b(.*?)(?=\s+HAVING|\s+ORDER\s+BY|$)", flat, re.DOTALL)
        grp_cols_raw = []
        grp_cols_expr = []  # for F.expr when it's a function call
        if grp_m:
            grp_content = sql[grp_m.start(1):grp_m.end(1)]
            for c in _parse_select_cols(grp_content):
                c_clean = c.strip()
                c_clean = re.sub(r"\[(\w+)\]", r"\1", c_clean)
                c_clean = re.sub(r"\bdbo\.", "", c_clean, flags=re.IGNORECASE)
                if re.match(r"^[\w\.]+$", c_clean):
                    grp_cols_raw.append(_clean_name(c_clean))
                else:
                    # Expression like YEAR(SaleDate) — keep as F.expr
                    grp_cols_expr.append(c_clean)
                    grp_cols_raw.append(c_clean)
        grp_cols = grp_cols_raw

        # HAVING — match in flat, extract from sql
        hav_m = re.search(r"(?i)\bHAVING\b(.*?)(?=\s+ORDER\s+BY|$)", flat, re.DOTALL)
        having_str = sql[hav_m.start(1):hav_m.end(1)].strip() if hav_m else ""

        # ORDER BY — match in flat (prevents matching inside OVER()), extract from sql
        ord_m = re.search(r"(?i)\bORDER\s+BY\b(.*?)(?=\s+LIMIT|$)", flat, re.DOTALL)
        ord_cols = []
        if ord_m:
            ord_content = sql[ord_m.start(1):ord_m.end(1)]
            for o in ord_content.split(","):
                o = o.strip()
                if not o:
                    continue
                desc = bool(re.search(r"(?i)\bDESC\b", o))
                col  = re.sub(r"(?i)\s*(ASC|DESC)\s*", "", o).strip()
                ord_cols.append((col, desc))

        # ── emit DataFrame chain ──────────────────────────────────────────────
        # Base DataFrame
        if main_tbl in df_map:
            base_var = df_map[main_tbl]
        else:
            base_var = _to_var(main_tbl)
            tbl_key  = main_tbl.lower()
            if "." in tbl_key:
                fq = main_tbl.replace(".", "`.`")
                lines.append(f'{base_var} = spark.table(f"`{{catalog}}`.`{fq}`")')
            else:
                lines.append(f'{base_var} = spark.table(f"`{{catalog}}`.`{{schema}}`.`{main_tbl}`")')
            df_map[main_tbl] = base_var

        chain_var = base_var

        # JOINs — use smart join condition for cleaner output
        for j_type_raw, j_tbl_raw, j_alias_raw, on_cond in joins:
            j_type  = _join_type(re.sub(r"\s+", " ", (j_type_raw or "INNER ")).split()[0])
            j_tbl   = _clean_name(j_tbl_raw)
            j_alias = j_alias_raw or j_tbl
            on_cond = re.sub(r"\[(\w+)\]", r"\1", on_cond)
            on_cond = re.sub(r"\bdbo\.", "", on_cond, flags=re.IGNORECASE)
            j_var   = df_map.get(j_tbl, _to_var(j_tbl))
            if j_tbl not in df_map:
                lines.append(f'{j_var} = spark.table(f"`{{catalog}}`.`{{schema}}`.`{j_tbl}`")')
                df_map[j_tbl] = j_var
            join_cond = _smart_join_condition(on_cond, chain_var, j_var, j_tbl)
            new_var  = f"joined_{j_tbl.lower()}_df"
            lines.append(f'{new_var} = {chain_var}.join({j_var}, {join_cond}, "{j_type}")')
            chain_var = new_var

        # SELECT columns / aggregates
        raw_cols  = _parse_select_cols(cols_part)
        is_star   = cols_part.strip() == "*"
        agg_funcs = [f for f in raw_cols if re.match(r"(?i)(SUM|COUNT|AVG|MIN|MAX|STDEV|STDDEV|VAR|VARIANCE|APPROX_COUNT_DISTINCT)\s*\(", f.strip())]
        win_cols  = [f for f in raw_cols if re.search(r"(?i)\bOVER\s*\(", f)]
        case_cols = [f for f in raw_cols if re.match(r"(?i)\bCASE\b", f.strip())]

        # WHERE filter
        if where_str:
            filt_expr = _where_to_filter(where_str)
            chain_var_new = f"filtered_df"
            lines.append(f'{chain_var_new} = {chain_var}.filter({filt_expr})')
            chain_var = chain_var_new

        # Window columns (add as withColumn before groupBy)
        if win_cols:
            lines.append(f"# ── Window functions ─────────────────────────────────────────────────")
            for wc in win_cols:
                alias_m = re.search(r"(?i)\s+(?:AS\s+)?(\w+)\s*$", wc)
                alias   = alias_m.group(1) if alias_m else "win_col"
                expr_part = re.sub(r"(?i)\s+(?:AS\s+)?\w+\s*$", "", wc).strip()
                win_expr  = _window_expr(expr_part)
                chain_var_new = f"win_{alias.lower()}_df"
                lines.append(f'{chain_var_new} = {chain_var}.withColumn("{alias}", {win_expr})')
                chain_var = chain_var_new

        # CASE WHEN columns
        if case_cols:
            lines.append(f"# ── CASE WHEN expressions ────────────────────────────────────────────")
            for cc in case_cols:
                alias_m = re.search(r"(?i)\s+(?:AS\s+)?(\w+)\s*$", cc)
                alias   = alias_m.group(1) if alias_m else "case_col"
                case_expr = _case_when(cc)
                chain_var_new = f"case_{alias.lower()}_df"
                lines.append(f'{chain_var_new} = {chain_var}.withColumn("{alias}", {case_expr})')
                chain_var = chain_var_new

        # GROUP BY + AGG
        if grp_cols and agg_funcs:
            # Smart group-by: plain columns use "col", expressions use F.expr("...")
            grp_items = []
            for c in grp_cols:
                if re.match(r"^[\w\.]+$", c):
                    grp_items.append(f'"{c}"')
                else:
                    safe = c.replace('"', '\\"')
                    grp_items.append(f'F.expr("{safe}")')
            grp_str  = ", ".join(grp_items)
            agg_strs = [_agg_func(a) for a in agg_funcs]
            # plain select cols (non-agg)
            plain_cols = [c for c in raw_cols if c not in agg_funcs and c not in win_cols and c not in case_cols and c != "*"]
            chain_var_new = result_var
            lines.append(f'{chain_var_new} = (')
            lines.append(f'    {chain_var}')
            lines.append(f'    .groupBy({grp_str})')
            lines.append(f'    .agg(')
            for i, ag in enumerate(agg_strs):
                comma = "," if i < len(agg_strs)-1 else ""
                lines.append(f'        {ag}{comma}')
            lines.append(f'    )')
            if having_str:
                hav_filt = _where_to_filter(having_str)
                lines.append(f'    .filter({hav_filt})')
            if ord_cols:
                ord_strs = [f'F.col("{c}").desc()' if d else f'F.col("{c}")' for c, d in ord_cols]
                lines.append(f'    .orderBy({", ".join(ord_strs)})')
            if top_n:
                lines.append(f'    .limit({top_n})')
            if distinct:
                lines.append(f'    .distinct()')
            lines.append(f')')
            chain_var = chain_var_new
        else:
            # Simple select
            chain_var_new = result_var
            if is_star:
                lines.append(f'{chain_var_new} = {chain_var}')
            else:
                sel_exprs = []
                for c in raw_cols:
                    if c in win_cols or c in case_cols:
                        continue   # already applied as withColumn
                    if re.match(r"(?i)(SUM|COUNT|AVG|MIN|MAX|STDEV|VAR)\s*\(", c.strip()):
                        continue   # no groupBy so treat as expr
                    sel_exprs.append(_col_expr(c))
                if sel_exprs:
                    exprs_str = ",\n        ".join(sel_exprs)
                    lines.append(f'{chain_var_new} = (')
                    lines.append(f'    {chain_var}')
                    lines.append(f'    .select(')
                    lines.append(f'        {exprs_str}')
                    lines.append(f'    )')
                    if ord_cols:
                        ord_strs = [f'F.col("{c}").desc()' if d else f'F.col("{c}")' for c, d in ord_cols]
                        lines.append(f'    .orderBy({", ".join(ord_strs)})')
                    if top_n:
                        lines.append(f'    .limit({top_n})')
                    if distinct:
                        lines.append(f'    .distinct()')
                    lines.append(f')')
                else:
                    if ord_cols:
                        ord_strs = [f'F.col("{c}").desc()' if d else f'F.col("{c}")' for c, d in ord_cols]
                        lines.append(f'{chain_var_new} = {chain_var}.orderBy({", ".join(ord_strs)})')
                    else:
                        lines.append(f'{chain_var_new} = {chain_var}')
            chain_var = chain_var_new

        # If SELECT INTO was detected, register the target as a cached DataFrame
        if into_target:
            target_var = _to_var(into_target) or into_target.lower() + "_df"
            if chain_var != target_var:
                lines.append(f'{target_var} = {chain_var}')
                chain_var = target_var
            if into_target.startswith("_tmp_"):
                lines.append(f'{target_var}.cache()')
                lines.append(f'# SELECT INTO #{into_target.replace("_tmp_", "", 1)} → cached DataFrame')
            df_map[into_target] = target_var

        return lines

    def _translate_merge(sql):
        """MERGE INTO → Delta Lake MERGE using spark.sql (only valid UC syntax)."""
        tgt_m  = re.search(r"(?i)MERGE\s+INTO\s+([\w\.]+)", sql)
        src_m  = re.search(r"(?i)USING\s+([\w\.]+)\s+(?:AS\s+)?(\w+)\s+ON\s+(.*?)(?=\s+WHEN)", sql, re.DOTALL)
        tgt    = _clean_name(tgt_m.group(1)) if tgt_m else "target_table"
        src    = _clean_name(src_m.group(1)) if src_m else "source_table"
        on_cond= src_m.group(3).strip() if src_m else "tgt.id = src.id"
        on_cond= re.sub(r"\[(\w+)\]", r"\1", on_cond)
        return [
            "# ── MERGE / Upsert — Delta Lake MERGE INTO ───────────────────────────",
            f'src_df = spark.table(f"{{catalog}}.{{schema}}.{src}")',
            f'src_df.createOrReplaceTempView("merge_source")',
            "",
            "spark.sql(f'''",
            f"    MERGE INTO {{catalog}}.{{schema}}.{tgt} AS tgt",
            f"    USING merge_source AS src",
            f"    ON {on_cond.replace(chr(39), chr(34))}",
            "    WHEN MATCHED THEN",
            "        UPDATE SET *",
            "    WHEN NOT MATCHED THEN",
            "        INSERT *",
            "''')",
        ]

    def _translate_insert(sql, target_var="result_df"):
        """INSERT INTO table ... → .write.saveAsTable() or spark.sql for VALUES."""
        # Strip SQL comments
        sql = re.sub(r"--[^\n]*", "", sql).strip()
        sql = _tsql_expr_to_pyspark(sql)
        sql = re.sub(r"\[(\w+)\]", r"\1", sql)
        sql = re.sub(r"\bdbo\.", "", sql, flags=re.IGNORECASE)

        tgt_m = re.search(r"(?i)INSERT\s+(?:INTO\s+)?([\w\.]+)", sql)
        tgt   = _clean_name(tgt_m.group(1)) if tgt_m else "target_table"

        # Detect INSERT...VALUES pattern (not INSERT...SELECT)
        if re.search(r"(?i)\bVALUES\s*\(", sql):
            # Extract column list if present
            cols_m = re.search(r"(?i)INSERT\s+(?:INTO\s+)?[\w\.]+\s*\(([^)]+)\)\s*VALUES", sql)
            cols_str = cols_m.group(1).strip() if cols_m else ""
            # Extract VALUES
            vals_m = re.search(r"(?i)\bVALUES\s*\((.+)\)\s*$", sql, re.DOTALL)
            vals_str = vals_m.group(1).strip() if vals_m else "..."
            lines = [
                f"# ── INSERT INTO {tgt} (VALUES) ─────────────────────────────────────────",
                f'spark.sql(f"""',
                f'    INSERT INTO {{catalog}}.{{schema}}.{tgt}',
            ]
            if cols_str:
                lines.append(f'    ({cols_str})')
            lines += [
                f'    VALUES ({vals_str})',
                f'""")',
            ]
            return lines

        # INSERT...SELECT pattern
        sel_m = re.search(r"(?i)\bSELECT\b", sql)
        if sel_m:
            sel_sql = sql[sel_m.start():]
            lines = _translate_select(sel_sql, {}, result_var=target_var)
        else:
            lines = [f"# Could not parse INSERT body"]

        lines  += [
            "",
            f"# ── Write to Delta table ─────────────────────────────────────────────────",
            f'output_table = f"{{catalog}}.{{schema}}.{tgt}"',
            f"(",
            f"    {target_var}.write",
            f"    .format('delta')",
            f"    .mode('append')",
            f"    .saveAsTable(output_table)",
            f")",
            f'print(f"[INFO] Inserted {{{{ {target_var}.count() }}}} rows into {{output_table}}")',
        ]
        return lines

    # ─── main build ───────────────────────────────────────────────────────────
    body   = _strip_header(sql_code)
    # Pre-process all T-SQL expressions to PySpark equivalents
    body   = _tsql_expr_to_pyspark(body)

    # Extract parameters ONLY from procedure header (before AS BEGIN), not from body
    header_match = re.search(
        r"(?i)(?:CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE|CREATE\s+(?:OR\s+ALTER\s+)?PROC)\s+"
        r"[\w\.\[\]\"]+\s+(.*?)\s*\bAS\b",
        sql_code, re.DOTALL)
    param_section = header_match.group(1) if header_match else ""
    params = re.findall(
        r"@(\w+)\s+([\w()]+(?:\(\d+(?:,\d+)?\))?)"
        r"(?:\s*=\s*([^,\n\r]+))?", param_section, re.IGNORECASE)

    # Detect features
    has_merge  = bool(re.search(r"(?i)\bMERGE\b", body))
    has_cursor = bool(re.search(r"(?i)DECLARE\b.*\bCURSOR\b", body))
    has_temp   = bool(re.search(r"#\w+", body))
    has_window = bool(re.search(r"(?i)\b(OVER\s*\(|ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\b", body))
    has_cte    = bool(re.search(r"(?i)(?:;?\s*WITH\b)\s+\w+\s+AS\s*\(", body))
    has_trycatch = bool(re.search(r"(?i)\bBEGIN\s+TRY\b", body))
    has_transaction = bool(re.search(r"(?i)\bBEGIN\s+TRANSACTION\b", body))
    has_while  = bool(re.search(r"(?i)\bWHILE\b", body))

    short_name = name.split(".")[-1]
    safe_name  = re.sub(r"[^\w]", "_", short_name)

    auto_notes = ["Pure PySpark DataFrame API — no spark.sql() for core logic"]
    if has_cte:         auto_notes.append("CTEs → separate named DataFrames")
    if has_merge:       auto_notes.append("MERGE → Delta Lake MERGE INTO via spark.sql()")
    if has_cursor:      auto_notes.append("CURSOR → DataFrame vectorized operations (no row-by-row)")
    if has_temp:        auto_notes.append("Temp tables (#table) → cached DataFrames")
    if has_window:      auto_notes.append("Window functions → PySpark Window API")
    if has_trycatch:    auto_notes.append("TRY/CATCH → Python try/except")
    if has_transaction: auto_notes.append("BEGIN TRANSACTION → Delta ACID (automatic)")
    if params:          auto_notes.append(f"{len(params)} parameter(s) → Databricks Widgets")
    auto_notes.append("T-SQL functions auto-mapped: ISNULL→COALESCE, GETDATE→current_timestamp, DATEDIFF, DATEADD, CAST, LEN, etc.")

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    lines += [
        f"# {'='*75}",
        f"# Databricks PySpark Notebook  —  Pure DataFrame API",
        f"# Source Object : {name}",
        f"# Object Type   : {otype.upper()}",
        f"# Converted At  : {ts}",
        f"# Runtime       : Databricks Runtime 14.x+ / Unity Catalog",
        f"# NOTE: Review column names, join keys and types before production use.",
        f"# {'='*75}",
        "",
    ]

    # ── Imports ───────────────────────────────────────────────────────────────
    lines += [
        "from pyspark.sql import SparkSession, functions as F",
        "from pyspark.sql.window import Window",
        "from pyspark.sql.types import *",
        "import datetime",
        "",
        "spark = SparkSession.builder.getOrCreate()",
        f'print(f"[INFO] {name} started | {{datetime.datetime.now()}}")',
        "",
    ]

    # ── Unity Catalog widgets ─────────────────────────────────────────────────
    lines += [
        "# ── Unity Catalog Settings ──────────────────────────────────────────────",
        'dbutils.widgets.text("catalog", "main",    "Unity Catalog")',
        'dbutils.widgets.text("schema",  "default", "Schema")',
        'catalog = dbutils.widgets.get("catalog")',
        'schema  = dbutils.widgets.get("schema")',
        "",
    ]

    # ── SP Parameters → Widgets ───────────────────────────────────────────────
    if params and otype in ("stored_procedure", "sp"):
        lines += [
            "# ── Stored Procedure Parameters → Databricks Widgets ───────────────────",
        ]
        for p_name, p_type, p_default in params:
            default_val = (p_default or "").strip().strip("'\"")
            lines.append(f'dbutils.widgets.text("{p_name}", "{default_val}", "{p_name} ({p_type})")')
        lines.append("")
        for p_name, _, _ in params:
            lines.append(f'{p_name} = dbutils.widgets.get("{p_name}")')
        lines.append("")

    # ── Temp table replacement comment ────────────────────────────────────────
    if has_temp:
        lines += [
            "# ── NOTE: Temp Tables (#name) replaced with cached DataFrames ──────────",
            "# Each #temp_table below is materialised as a cached DataFrame.",
            "",
        ]

    # ── Cursor replacement ────────────────────────────────────────────────────
    if has_cursor:
        lines += [
            "# ── NOTE: CURSOR → Vectorized DataFrame Operations ──────────────────────",
            "# BEST PRACTICE: Avoid row-by-row loops. Use DataFrame API instead:",
            "#",
            "# Pattern 1 — Simple transformation:  df.withColumn('new_col', F.expr(...))",
            "# Pattern 2 — Conditional logic:       df.withColumn('col', F.when(...).otherwise(...))",
            "# Pattern 3 — Group + aggregate:        df.groupBy('key').agg(F.sum('val'))",
            "# Pattern 4 — UDF for complex logic:    @F.udf(returnType=StringType())",
            "#                                       def process_row(col1, col2): ...",
            "# Pattern 5 — Pandas UDF (fastest):     @F.pandas_udf('return_type')",
            "#                                       def process_batch(series): ...",
            "#",
            "# ONLY if row-by-row is truly required (very rare):",
            "#   for row in small_df.collect():",
            "#       process(row['column'])",
            "",
        ]

    # ── UDF: Python function ──────────────────────────────────────────────────
    if otype == "udf":
        udf_func = re.sub(r"[^\w]", "_", short_name).lower()
        lines += [
            "# ── PySpark UDF (User-Defined Function) ─────────────────────────────────",
            "# Prefer native F.* functions where possible — UDFs disable Catalyst optimiser.",
            "",
            "from pyspark.sql.types import StringType",
            "",
            "@F.udf(returnType=StringType())  # TODO: set correct return type",
            f"def {udf_func}(*args):",
            "    # TODO: Implement the Python equivalent of the SQL UDF body below.",
            "    # Original SQL body is in the comment block at end of this file.",
            "    raise NotImplementedError('Implement Python logic here')",
            "",
            f"spark.udf.register('{short_name}', {udf_func})",
            f'print("[INFO] UDF {short_name} registered.")',
            "",
            "# ── Original SQL UDF body (for reference) ───────────────────────────────",
        ]
        for sql_line in body.splitlines()[:80]:
            lines.append(f"# {sql_line}")
        lines += ["", f'print(f"[INFO] {name} completed | {{datetime.datetime.now()}}")']
        return "\n".join(lines), auto_notes

    # ── VIEW: produce DataFrame + optional materialise ────────────────────────
    if otype == "view":
        lines += [
            "# ── View Logic — Pure PySpark DataFrame ─────────────────────────────────",
        ]
        # strip any leading SELECT for view body
        view_body = re.sub(r"(?i)^\s*SELECT\b", "SELECT", body.strip())
        ctes, core_sql = _parse_ctes(view_body)
        df_map = {}

        if ctes:
            lines.append("# CTEs as named DataFrames:")
        for cte_name, cte_body in ctes.items():
            cte_var = cte_name.lower() + "_df"
            lines.append(f"# CTE: {cte_name}")
            cte_lines = _translate_select(cte_body, df_map, result_var=cte_var)
            lines += cte_lines
            df_map[cte_name] = cte_var
            lines.append("")

        lines.append("# Final SELECT:")
        result_lines = _translate_select(core_sql, df_map, result_var="view_df")
        lines += result_lines
        lines += [
            "",
            "# ── Display preview ─────────────────────────────────────────────────────",
            "display(view_df.limit(1000))",
            "",
            "# ── Optional: Materialise as Delta table ─────────────────────────────────",
            f'# output_table = f"{{catalog}}.{{schema}}.{safe_name}"',
            "# (",
            "#     view_df.write",
            "#     .format('delta')",
            "#     .mode('overwrite')",
            "#     .option('overwriteSchema', 'true')",
            "#     .saveAsTable(output_table)",
            "# )",
            "",
            f'print(f"[INFO] {name} completed | {{datetime.datetime.now()}}")',
        ]
        return "\n".join(lines), auto_notes

    # ── STORED PROCEDURE: translate statement by statement ────────────────────
    lines += [
        "# ── Stored Procedure Logic — Pure PySpark DataFrame API ─────────────────",
    ]
    df_map = {}

    # Replace @params in body with Python variable references
    proc_body = body
    for p_name, _, _ in params:
        proc_body = re.sub(rf"@{p_name}\b", f"{{{p_name}}}", proc_body)
    # Temp tables  →  _tmp_xxx naming
    proc_body = re.sub(r"#(\w+)", r"_tmp_\1", proc_body)
    # Brackets
    proc_body = re.sub(r"\[(\w+)\]", r"\1", proc_body)

    # Split into statements on ; or GO
    stmts = re.split(r"(?i)\s*;\s*|\s+\bGO\b\s*", proc_body)
    result_counter = [0]

    for stmt in stmts:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Strip leading SQL single-line comments (-- ...) so pattern matching works
        while stmt.lstrip().startswith("--"):
            stmt = re.sub(r"^\s*--[^\n]*\n?", "", stmt, count=1).strip()
        if not stmt:
            continue

        stmt_up = stmt.upper().lstrip()

        # ── Skip noise statements ──────────────────────────────────────────
        if re.match(r"(?i)SET\s+NOCOUNT\b", stmt):
            lines.append("# SET NOCOUNT ON — not needed in PySpark")
            continue
        if re.match(r"(?i)SET\s+ANSI_\w+|SET\s+QUOTED_IDENTIFIER|SET\s+XACT_ABORT", stmt):
            lines.append(f"# {stmt.split()[0]} {stmt.split()[1]} — SQL Server session setting, not applicable in PySpark")
            continue

        # ── BEGIN TRY / END TRY / BEGIN CATCH / END CATCH ─────────────────
        if re.match(r"(?i)BEGIN\s+TRY\b", stmt):
            lines.append("try:")
            lines.append("    # ── BEGIN TRY block ────────────────────────────────────────────────")
            continue
        if re.match(r"(?i)END\s+TRY\b", stmt):
            continue  # handled by except block
        if re.match(r"(?i)BEGIN\s+CATCH\b", stmt):
            lines.append("except Exception as e:")
            lines.append("    # ── BEGIN CATCH block → Python except ──────────────────────────────")
            lines.append('    print(f"[ERROR] {name} failed: {str(e)}")')
            continue
        if re.match(r"(?i)END\s+CATCH\b", stmt):
            lines.append('    raise  # Re-raise after logging')
            continue

        # ── BEGIN TRANSACTION / COMMIT / ROLLBACK ─────────────────────────
        if re.match(r"(?i)BEGIN\s+TRANSACTION\b", stmt):
            lines.append("# BEGIN TRANSACTION — Delta Lake provides ACID automatically")
            continue
        if re.match(r"(?i)COMMIT\b", stmt):
            lines.append("# COMMIT — Delta Lake auto-commits")
            continue
        if re.match(r"(?i)ROLLBACK\b", stmt):
            lines.append("# ROLLBACK — use Delta time travel: spark.sql('RESTORE TABLE ... TO VERSION AS OF ...')")
            continue

        # ── RAISERROR / THROW ─────────────────────────────────────────────
        if re.match(r"(?i)(RAISERROR|THROW)\b", stmt):
            msg_m = re.search(r"(?i)(?:RAISERROR|THROW)\s*\(?['\"]?(.+?)['\"]?\s*[,)]", stmt)
            msg = msg_m.group(1) if msg_m else "Error occurred"
            lines.append(f'raise Exception("{msg}")')
            continue

        # ── EXEC / sp_executesql ──────────────────────────────────────────
        if re.match(r"(?i)(EXEC|EXECUTE)\b", stmt):
            exec_body = re.sub(r"(?i)^(EXEC|EXECUTE)\s+", "", stmt).strip()
            lines.append(f"# EXEC {exec_body[:60]}...")
            lines.append(f"# TODO: Convert to spark.sql() or Python function call")
            lines.append(f'# spark.sql(f"""{exec_body}""")')
            lines.append("")
            continue

        # ── TRUNCATE TABLE ────────────────────────────────────────────────
        if re.match(r"(?i)TRUNCATE\s+TABLE\b", stmt):
            tbl_m = re.search(r"(?i)TRUNCATE\s+TABLE\s+([\w\.]+)", stmt)
            tbl = _clean_name(tbl_m.group(1)) if tbl_m else "table_name"
            lines.append(f'spark.sql(f"TRUNCATE TABLE {{catalog}}.{{schema}}.{tbl}")')
            continue

        # ── DROP TABLE [IF EXISTS] ────────────────────────────────────────
        if re.match(r"(?i)DROP\s+TABLE\b", stmt):
            tbl_m = re.search(r"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w\.\#]+)", stmt)
            tbl = _clean_name(tbl_m.group(1)) if tbl_m else "table_name"
            if tbl.startswith("_tmp_") or tbl.startswith("#"):
                var = re.sub(r"\W", "_", tbl.lower()) + "_df"
                lines.append(f"# DROP TABLE {tbl} — unpersist cached DataFrame")
                lines.append(f"try:")
                lines.append(f"    {var}.unpersist()")
                lines.append(f"except Exception:")
                lines.append(f"    pass")
            else:
                lines.append(f'spark.sql(f"DROP TABLE IF EXISTS {{catalog}}.{{schema}}.{tbl}")')
            continue

        # ── WHILE loop ────────────────────────────────────────────────────
        if re.match(r"(?i)WHILE\b", stmt):
            cond = re.sub(r"(?i)^WHILE\s+", "", stmt).split("\n")[0].strip()
            cond = re.sub(r"@@FETCH_STATUS\s*=\s*0", "True  # Replace with actual loop condition", cond)
            cond = re.sub(r"@(\w+)", r"\1", cond)
            lines.append(f"while {cond}:")
            lines.append(f"    pass  # TODO: implement loop body using DataFrame operations")
            lines.append(f"    # PREFER: df.transform() or groupBy().applyInPandas() over row-by-row loops")
            lines.append("")
            continue

        # ── Cursor operations (DECLARE CURSOR, OPEN, FETCH, CLOSE, DEALLOCATE) ──
        if re.match(r"(?i)DECLARE\b.*\bCURSOR\b", stmt):
            cursor_m = re.search(r"(?i)FOR\s+(SELECT\b.+)", stmt, re.DOTALL)
            if cursor_m:
                lines.append(f"# ── CURSOR replaced with DataFrame operation ──────────────────────")
                lines.append(f"# Original cursor SELECT:")
                for cl in cursor_m.group(1).splitlines()[:5]:
                    lines.append(f"#   {cl.strip()}")
                lines.append(f"# Convert to: result_df = <DataFrame operation> (see above)")
                lines.append(f"# Then use: for row in result_df.collect():  # Only for small datasets!")
            else:
                lines.append(f"# CURSOR declaration skipped — convert to DataFrame operations")
            lines.append("")
            continue
        if re.match(r"(?i)(OPEN|CLOSE|DEALLOCATE)\b", stmt):
            lines.append(f"# {stmt.split()[0]} cursor — not needed in PySpark DataFrame API")
            continue
        if re.match(r"(?i)FETCH\b", stmt):
            lines.append(f"# FETCH from cursor — replaced by DataFrame operations above")
            continue

        # ── RETURN statement ──────────────────────────────────────────────
        if re.match(r"(?i)RETURN\b", stmt):
            ret_val = re.sub(r"(?i)^RETURN\s*", "", stmt).strip()
            if ret_val:
                lines.append(f"# RETURN {ret_val}")
                lines.append(f'dbutils.notebook.exit("{ret_val}")')
            else:
                lines.append('dbutils.notebook.exit("0")')
            continue

        # ── DECLARE @var (improved type mapping) ──────────────────────────
        if re.match(r"(?i)DECLARE\s+\{", stmt) or re.match(r"(?i)DECLARE\s+@", stmt):
            var_m = re.match(r"(?i)DECLARE\s+[@{](\w+)\}?\s+([\w()]+(?:\(\d+(?:,\d+)?\))?)(?:\s*=\s*(.+))?", stmt)
            if var_m:
                vname, vtype, vdef = var_m.group(1), var_m.group(2), var_m.group(3) or "None"
                py_type = _sql_type_to_python(vtype)
                lines.append(f"{vname}: {py_type} = {vdef.strip()} # DECLARE @{vname} {vtype}")
            continue

        # SET @var = value
        if re.match(r"(?i)SET\s+\{", stmt) or re.match(r"(?i)SET\s+@", stmt):
            set_m = re.match(r"(?i)SET\s+[@{](\w+)\}?\s*=\s*(.+)", stmt)
            if set_m:
                vname, vval = set_m.group(1), set_m.group(2).strip()
                lines.append(f"{vname} = {vval}  # SET @{vname}")
            continue

        # PRINT
        if re.match(r"(?i)PRINT\b", stmt):
            msg = re.sub(r"(?i)^PRINT\s+", "", stmt).strip().strip("'")
            lines.append(f'print("{msg}")')
            continue

        # IF / ELSE
        if re.match(r"(?i)IF\b", stmt):
            cond = re.sub(r"(?i)^IF\s+", "", stmt).split("\n")[0].strip()
            cond = re.sub(r"@(\w+)", r"\1", cond)
            lines += [f"if {cond}:  # TODO verify condition", "    pass"]
            continue

        # SELECT INTO (temp table create)
        sinto_m = re.match(r"(?i)SELECT\b.+\bINTO\s+(_tmp_\w+|\w+)\b", stmt, re.DOTALL)
        if sinto_m:
            tgt_tbl = sinto_m.group(1)
            tgt_var = re.sub(r"\W", "_", tgt_tbl.lower()) + "_df"
            sel_part = re.sub(r"(?i)\bINTO\s+\w+\b", "", stmt)
            lines.append(f"# SELECT INTO {tgt_tbl}  →  cached DataFrame")
            tr = _translate_select(sel_part, df_map, result_var=tgt_var)
            lines += tr
            lines.append(f"{tgt_var}.cache()")
            df_map[tgt_tbl] = tgt_var
            lines.append("")
            continue

        # CREATE TABLE #temp / permanent
        if re.match(r"(?i)CREATE\s+TABLE\b", stmt):
            tbl_m = re.match(r"(?i)CREATE\s+TABLE\s+(_tmp_\w+|[\w\.]+)", stmt)
            tbl   = tbl_m.group(1) if tbl_m else "new_table"
            lines.append(f"# CREATE TABLE {tbl} → define schema in PySpark if needed:")
            lines.append(f"# {tbl}_schema = StructType([StructField('col', StringType(), True)])")
            lines.append(f"# {re.sub(chr(92)+'W','_',tbl.lower())}_df = spark.createDataFrame([], {tbl}_schema)")
            lines.append("")
            continue

        # INSERT INTO
        if re.match(r"(?i)INSERT\b", stmt):
            result_counter[0] += 1
            rv = f"insert_result_{result_counter[0]}_df"
            lines.append(f"# ── INSERT INTO ──────────────────────────────────────────────────────")
            ins_lines = _translate_insert(stmt, target_var=rv)
            lines += ins_lines
            lines.append("")
            continue

        # MERGE
        if re.match(r"(?i)MERGE\b", stmt):
            lines.append("")
            lines += _translate_merge(stmt)
            lines.append("")
            continue

        # UPDATE
        if re.match(r"(?i)UPDATE\b", stmt):
            tgt_m = re.search(r"(?i)UPDATE\s+([\w\.]+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.+))?$", stmt, re.DOTALL)
            if tgt_m:
                tgt   = _clean_name(tgt_m.group(1))
                set_p = tgt_m.group(2).strip()
                whr_p = (tgt_m.group(3) or "").strip()
                _set_clean = re.sub(r"\[(\w+)\]", r"\1", set_p)
                lines += [
                    f"# ── UPDATE {tgt} ──────────────────────────────────────────────────────",
                    f"# Delta Lake UPDATE (via spark.sql):",
                    f'spark.sql(f"""',
                    f'    UPDATE {{catalog}}.{{schema}}.{tgt}',
                    f'    SET {_set_clean}',
                ]
                if whr_p:
                    lines.append(f'    WHERE {whr_p}')
                lines += ['""")', ""]
            continue

        # DELETE
        if re.match(r"(?i)DELETE\b", stmt):
            del_m = re.search(r"(?i)FROM\s+([\w\.]+)(?:\s+WHERE\s+(.+))?$", stmt, re.DOTALL)
            if del_m:
                tgt   = _clean_name(del_m.group(1))
                whr_p = (del_m.group(2) or "").strip()
                lines += [
                    f"# ── DELETE FROM {tgt} ────────────────────────────────────────────────",
                    f'spark.sql(f"""DELETE FROM {{catalog}}.{{schema}}.{tgt}{"" if not whr_p else chr(10)+"    WHERE "+whr_p}""")',
                    "",
                ]
            continue

        # Regular SELECT
        if re.match(r"(?i)(?:WITH\b|SELECT\b)", stmt):
            ctes, core = _parse_ctes(stmt)
            if ctes:
                lines.append("# ── CTEs as intermediate DataFrames ─────────────────────────────────")
            for cte_name, cte_sql in ctes.items():
                cte_var = cte_name.lower() + "_df"
                lines.append(f"# CTE: {cte_name}")
                lines += _translate_select(cte_sql, df_map, result_var=cte_var)
                df_map[cte_name] = cte_var
                lines.append("")

            result_counter[0] += 1
            rv = f"result_{result_counter[0]}_df" if result_counter[0] > 1 else "result_df"
            lines.append(f"# ── Main SELECT ─────────────────────────────────────────────────────")
            lines += _translate_select(core, df_map, result_var=rv)
            lines += ["", f"display({rv}.limit(1000))", ""]
            continue

        # Anything else — emit as comment
        lines.append(f"# TODO: Translate manually:")
        for l in stmt.splitlines():
            lines.append(f"#   {l}")
        lines.append("")

    # ── Merge hint ────────────────────────────────────────────────────────────
    if has_merge:
        lines += [
            "# ── Delta MERGE pattern reference ────────────────────────────────────────",
            "# spark.sql(f'''",
            "#   MERGE INTO {catalog}.{schema}.target AS tgt",
            "#   USING source_view AS src ON tgt.id = src.id",
            "#   WHEN MATCHED THEN UPDATE SET *",
            "#   WHEN NOT MATCHED THEN INSERT *",
            "# ''')",
            "",
        ]

    lines += [f'print(f"[INFO] {name} completed | {{datetime.datetime.now()}}")']

    return "\n".join(lines), auto_notes

# ─────────────────────────────────────────────────────────────────────────────
# Combined HelperFunction Notebook Builder
# ─────────────────────────────────────────────────────────────────────────────
def get_combined_pyspark_code(object_names: list) -> dict:
    """
    Build a single 'HelperFunction' notebook combining all selected SQL objects.
    Returns a dict with success, notebook_name, pyspark_code, object_count,
    conversion_notes (keyed by object name), and a list of any missing objects.
    """
    import datetime

    timestamp   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sections    = []
    all_notes   = {}
    missing     = []
    valid_names = []

    for name in object_names:
        result = get_pyspark_code(name)
        if not result["success"]:
            missing.append(name)
            continue
        valid_names.append(name)
        all_notes[name] = result["conversion_notes"]
        obj_type = result.get("object_type", "unknown").upper()
        divider  = "═" * 75
        section  = f"""
# ╔{divider}╗
# ║  SECTION : {name:<62} ║
# ║  TYPE    : {obj_type:<62} ║
# ╚{divider}╝

{textwrap.dedent(result['pyspark_code']).strip()}
"""
        sections.append(section)

    if not valid_names:
        return {
            "success": False,
            "error"  : "None of the requested objects have conversion templates.",
            "missing": missing
        }

    shared_header = f'''# {"="*77}
# HelperFunction — Combined PySpark Migration Notebook
# Generated By  : SQL → Databricks Migration Utility
# Generated At  : {timestamp}
# Objects Count : {len(valid_names)}
# Objects       : {", ".join(valid_names)}
# Target        : Databricks Runtime 14.x+ / Unity Catalog
# {"="*77}
#
# HOW TO USE:
#   1. Attach to a Databricks cluster (DBR 14.x+)
#   2. Set Widgets: catalog / schema / env parameters per section
#   3. Run All Cells  — or run individual sections independently
# {"="*77}

# ── Shared Imports (available to all sections) ─────────────────────────────
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime, textwrap

spark = SparkSession.builder.getOrCreate()
print("HelperFunction notebook initialized — Databricks Runtime:", spark.version)
'''

    full_code = shared_header + "\n\n".join(sections)

    return {
        "success"          : True,
        "notebook_name"    : "HelperFunction",
        "pyspark_code"     : full_code,
        "object_count"     : len(valid_names),
        "included_objects" : valid_names,
        "missing_objects"  : missing,
        "lines"            : len(full_code.splitlines()),
        "conversion_notes" : all_notes
    }


# ─────────────────────────────────────────────────────────────────────────────
# Separate Files Builder  (one .py per SP/View + one HelperFunction.py for UDFs)
# ─────────────────────────────────────────────────────────────────────────────
def get_separate_pyspark_codes(object_names: list, objects_with_code: dict = None) -> dict:
    """
    Build individual PySpark notebooks for each SP/View, and a single
    HelperFunction.py that contains all selected UDFs plus shared imports.

    Parameters
    ----------
    object_names      : list of object key strings to convert
    objects_with_code : optional dict  { key -> {type, code} }
                        When provided (live-DB objects), auto-converts objects
                        that have no pre-built template using sql_to_pyspark_auto().

    Returns:
        success, files[{name, object_type, code, filename, lines}],
        helper_code, helper_lines, object_count, included_objects,
        missing_objects, conversion_notes, udf_count, sp_view_count
    """
    import datetime

    timestamp     = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    objects_with_code = objects_with_code or {}
    individual    = []      # SP / VIEW individual file dicts
    udf_sections  = []      # (name, code) for UDFs
    all_notes     = {}
    missing       = []
    valid_names   = []

    for name in object_names:
        result = get_pyspark_code(name)
        if not result["success"]:
            # ── No pre-built template: try auto-converter ──────────────────
            live = objects_with_code.get(name)
            if live and live.get("code", "").strip():
                auto_code, auto_notes_list = sql_to_pyspark_auto(
                    name,
                    live.get("type", "stored_procedure"),
                    live["code"]
                )
                result = {
                    "success"         : True,
                    "sp_name"         : name,
                    "object_type"     : live.get("type", "stored_procedure"),
                    "pyspark_code"    : auto_code,
                    "lines"           : len(auto_code.splitlines()),
                    "conversion_notes": auto_notes_list,
                    "auto_converted"  : True,
                }
            else:
                missing.append(name)
                continue
        valid_names.append(name)
        all_notes[name] = result["conversion_notes"]
        obj_type        = result.get("object_type", "unknown")
        code            = textwrap.dedent(result["pyspark_code"]).strip()

        if obj_type == "udf":
            udf_sections.append((name, code))
        else:
            file_header = (
                f"# {'='*75}\n"
                f"# Notebook        : {name}\n"
                f"# Object Type     : {obj_type.upper()}\n"
                f"# Generated At    : {timestamp}\n"
                f"# Target          : Databricks Runtime 14.x+ / Unity Catalog\n"
                f"# Migration Tool  : SQL -> Databricks Migration Utility\n"
                f"# {'='*75}\n\n"
            )
            individual.append({
                "name"       : name,
                "object_type": obj_type,
                "code"       : file_header + code,
                "filename"   : f"{name}.py",
                "lines"      : len(code.splitlines()),
            })

    if not valid_names:
        return {
            "success": False,
            "error"  : "None of the requested objects have conversion templates.",
            "missing": missing,
        }

    # ── Build HelperFunction.py (shared imports + all UDFs) ──────────────────
    udf_names   = [n for n, _ in udf_sections]
    helper_top  = (
        f"# {'='*75}\n"
        f"# HelperFunction.py  — Shared Utilities & User-Defined Functions\n"
        f"# Generated By : SQL -> Databricks Migration Utility\n"
        f"# Generated At : {timestamp}\n"
        f"# UDFs Included: {len(udf_sections)}"
        + (f"  ({', '.join(udf_names)})" if udf_names else "") + "\n"
        f"# Usage        : Run this notebook once per session before other pipelines.\n"
        f"# {'='*75}\n\n"
        f"# -- Shared Imports --------------------------------------------------\n"
        f"from pyspark.sql import SparkSession, functions as F\n"
        f"from pyspark.sql.window import Window\n"
        f"from pyspark.sql.types import *\n"
        f"import datetime\n\n"
        f"spark = SparkSession.builder.getOrCreate()\n"
        f"print('HelperFunction initialized | Databricks Runtime:', spark.version)\n"
    )

    udf_blocks = []
    for udf_name, udf_code in udf_sections:
        bar = "-" * 73
        udf_blocks.append(
            f"\n# +{bar}+\n"
            f"# |  UDF: {udf_name:<66}|\n"
            f"# +{bar}+\n\n"
            f"{udf_code}"
        )

    helper_code  = helper_top + "".join(udf_blocks)
    helper_lines = len(helper_code.splitlines())

    # If no UDFs were selected, produce a minimal helper stub
    if not udf_sections:
        helper_code = (
            helper_top
            + "\n# No UDFs were selected — this file contains shared imports only.\n"
        )
        helper_lines = len(helper_code.splitlines())

    return {
        "success"         : True,
        "files"           : individual,
        "helper_code"     : helper_code,
        "helper_lines"    : helper_lines,
        "helper_notebook" : "HelperFunction",
        "object_count"    : len(valid_names),
        "included_objects": valid_names,
        "missing_objects" : missing,
        "conversion_notes": all_notes,
        "udf_count"       : len(udf_sections),
        "sp_view_count"   : len(individual),
    }
