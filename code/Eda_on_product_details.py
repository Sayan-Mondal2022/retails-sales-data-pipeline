# Databricks notebook source
# MAGIC %md
# MAGIC # EDA on Product Details
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC The objective of this analysis is to understand the structure, quality, and distribution of the Product Details dataset. The analysis focuses on:
# MAGIC
# MAGIC - Understanding the dataset schema
# MAGIC - Identifying missing values
# MAGIC - Detecting duplicate records
# MAGIC - Validating product attributes
# MAGIC - Validating pricing information
# MAGIC - Analyzing category distribution
# MAGIC - Identifying potential data quality issues
# MAGIC
# MAGIC The insights derived from this analysis will help ensure the dataset is reliable for downstream reporting and business analytics.

# COMMAND ----------

# Setting up the Path

# Set the storage account key in spark config
spark.conf.set(
    "fs.azure.account.key.retailsalesresources.blob.core.windows.net",
    dbutils.secrets.get("databricksScope", "storageaccount-secrets")  
)

# File path for raw-data container
RAW_DATA_PATH = "wasbs://raw-data@retailsalesresources.blob.core.windows.net"

# Loading the CSV files
product_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_DATA_PATH}/product_details.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC Before performing any analysis, it is important to understand the structure of the dataset, including the number of records, columns, and data types.
# MAGIC
# MAGIC The results below provide a high-level overview of the Product Details dataset.

# COMMAND ----------

## Total row counts
print("Total rows are: ",product_df.count())


print("\n","=" *40)
print("Product details schema:")
product_df.printSchema()


print("\n","=" *40)
print("Total number of columns\n",len(product_df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descriptive Statistics
# MAGIC
# MAGIC Descriptive statistics provide a summary of the numerical attributes in the dataset and help identify unusual values, missing information, and potential outliers.
# MAGIC
# MAGIC The summary statistics for the dataset are shown below.

# COMMAND ----------

print("Product description\n")
product_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Value Analysis
# MAGIC
# MAGIC Missing values can affect both analytical results and downstream processing.
# MAGIC
# MAGIC The following analysis identifies the number of missing values present in each column.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Missing values per column
print("PRINTING MISSING VALUE COUNT PER COLUMNS")
product_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in product_df.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC No missing values were identified in any column of the Product Details dataset.
# MAGIC
# MAGIC This indicates that the dataset is complete and does not require any missing value treatment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distinct Value Analysis
# MAGIC
# MAGIC Distinct value analysis helps understand the uniqueness and distribution of values within each column.
# MAGIC
# MAGIC The results below show the distinct values present across the dataset.

# COMMAND ----------

for col in product_df.columns:
    print("\n","=" *40)
    print(f"Column name: {col}\n")
    
    # Displaying the columns
    product_df.select(col).distinct().show()

    # Displaying the per column count
    print(f"\nUnique {col} values are: ", product_df.select(col).distinct().count())

# COMMAND ----------

duplicate_count = product_df.count() - product_df.dropDuplicates().count()
print("Duplicate rows:", duplicate_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC No duplicate records were identified in the Product Details dataset.
# MAGIC
# MAGIC This indicates that each record represents a unique product entry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product ID Validation
# MAGIC
# MAGIC The Product ID serves as the primary identifier for products.
# MAGIC
# MAGIC This validation verifies whether each Product ID is unique and ensures referential integrity across datasets.

# COMMAND ----------

from pyspark.sql.functions import count

product_df.groupBy("product_id") \
    .agg(count("*").alias("cnt")) \
    .filter("cnt > 1") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC No duplicate Product IDs were identified.
# MAGIC
# MAGIC This confirms that each product is uniquely represented within the dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Name Validation
# MAGIC
# MAGIC The Product Name column was analyzed to identify missing values, blank entries, and potential inconsistencies.
# MAGIC
# MAGIC The validation results are shown below.

# COMMAND ----------

from pyspark.sql.functions import col  

product_df.filter(
    (col("product_name").isNull()) |
    (col("product_name") == "")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC No missing or blank product names were identified.
# MAGIC
# MAGIC The Product Name column appears complete and suitable for business reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category Validation
# MAGIC
# MAGIC The Category column was analyzed to understand the distribution of products across categories and identify potential inconsistencies.
# MAGIC
# MAGIC The category distribution results are shown below.

# COMMAND ----------

product_df.groupBy("category") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC The category values appear consistent and do not contain any obvious anomalies or invalid entries.
# MAGIC
# MAGIC The distribution provides a clear view of product concentration across categories.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Validation
# MAGIC
# MAGIC The Price column was validated to identify invalid values such as negative prices, zero prices, and other pricing anomalies.
# MAGIC
# MAGIC The validation results are shown below.

# COMMAND ----------

# Checking for negetive prices
product_df.filter(col("price") < 0).show()

# COMMAND ----------

# Cheking for prices equal to zero
product_df.filter(col("price") == 0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Outlier Analysis
# MAGIC
# MAGIC Outlier analysis was performed to identify products with unusually high or low prices.
# MAGIC
# MAGIC These observations help detect potential data entry errors and understand pricing variation within the catalog.

# COMMAND ----------

product_df.select("price").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC The summary statistics provide visibility into the overall price distribution and help identify potential outliers requiring further investigation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category Distribution Analysis
# MAGIC
# MAGIC Understanding category distribution helps identify dominant product groups and assess the composition of the product catalog.
# MAGIC
# MAGIC The category distribution visualization is shown below.

# COMMAND ----------

import matplotlib.pyplot as plt

category_counts = (
    product_df.groupBy("category")
    .count()
    .toPandas()
)

category_counts.plot(
    x="category",
    y="count",
    kind="bar"
)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Distribution Analysis
# MAGIC
# MAGIC Price distribution analysis helps understand pricing patterns, skewness, and potential concentration of products within specific price ranges.
# MAGIC
# MAGIC The distribution visualization is shown below.

# COMMAND ----------

price_pd = product_df.select("price").toPandas()

price_pd["price"].hist()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Conclusion
# MAGIC
# MAGIC The exploratory data analysis of the Product Details dataset has been successfully completed.
# MAGIC
# MAGIC ### Key Findings
# MAGIC
# MAGIC - The dataset schema and structure were reviewed and validated.
# MAGIC - No significant missing value issues were identified.
# MAGIC - Duplicate record checks were performed to ensure data quality.
# MAGIC - Product IDs were validated to confirm uniqueness.
# MAGIC - Product names and categories were reviewed for completeness and consistency.
# MAGIC - Price validation was performed to identify invalid pricing records.
# MAGIC - Outlier analysis was conducted to assess price distribution.
# MAGIC - Category and price distribution analyses provided insights into the composition of the product catalog.
# MAGIC
# MAGIC ### Outcome
# MAGIC
# MAGIC The Product Details dataset has been thoroughly profiled and validated. The results indicate that the dataset is suitable for downstream reporting, analytics, and business intelligence workloads.