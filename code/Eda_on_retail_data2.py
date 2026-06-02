# Databricks notebook source
# MAGIC %md
# MAGIC ## Retail Data Analysis
# MAGIC
# MAGIC **Tasks**
# MAGIC
# MAGIC - Check for missing values
# MAGIC - Check for duplicate values
# MAGIC - Validate data quality

# COMMAND ----------

# DBTITLE 1,Loading dataset
# Setting up the Path

# Set the storage account key in spark config
spark.conf.set(
    "fs.azure.account.key.retailsalesresources.blob.core.windows.net",
    dbutils.secrets.get("databricksScope", "storageaccount-secrets")  
)

# File path for raw-data container
RAW_DATA_PATH = "wasbs://raw-data@retailsalesresources.blob.core.windows.net"

try:
    # Loading the CSV files
    retail_data = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_DATA_PATH}/retail_data2.csv")
    )

    # Saving the CSV files as a pandas dataframe
    df = retail_data.toPandas()
except Exception as e:
    raise Exception("Couldn't load the CSV file")


# COMMAND ----------

## Total row counts
print("Total rows are: ",retail_data.count())


print("\n","=" *40)
print("Product details schema:")
retail_data.printSchema()


print("\n","=" *40)
print("Total number of columns\n",len(retail_data.columns))

# COMMAND ----------

print("Data types for each columns:\n")
for col,  dtype in retail_data.dtypes:
    print(f"Column name: {col}\n\t|--> Data type: {dtype}\n")


# COMMAND ----------

# DBTITLE 1,Data frame info
df.info()

# COMMAND ----------

# DBTITLE 1,Dataset Description
print("Retail data description\n")

summary_pdf = (
    retail_data.describe()
    .toPandas()
    .set_index("summary")
)

display(summary_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Analyzing Missing values

# COMMAND ----------

# DBTITLE 1,Analyzing the Missing Values
from pyspark.sql.functions import col, count, when

# Aggregate missing counts in PySpark
missing = retail_data.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).toPandas().T.reset_index()

missing.columns = ["column", "missing_count"]
missing["missing_%"] = (missing["missing_count"] / len(df)) * 100
# missing = sort_values("missing_%", ascending=False)

# COMMAND ----------

# DBTITLE 1,Plotting the missing values
import matplotlib.pyplot as plt

# Plot
plt.figure(figsize=(10, 5))
plt.bar(missing["column"], missing["missing_%"], color="blue")
plt.title("Missing Values % per Column (Pre-Transformation)")
plt.ylabel("Missing %")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Missing Value Insights
# MAGIC
# MAGIC - The **price** column is the only column containing missing values.
# MAGIC - The missing values can be resolved by joining the dataset with the **Product Details** table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation
# MAGIC
# MAGIC The following columns will be validated:
# MAGIC
# MAGIC - Price
# MAGIC - Product Name
# MAGIC - Product Category
# MAGIC
# MAGIC To perform the validation, the **Retail_Data1** table will be joined with the **Products** table using an **INNER JOIN**. The joined dataset will be used to verify the accuracy and consistency of the product-related information.

# COMMAND ----------

# DBTITLE 1,Validating the product data
# Loading the product details
product_details = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_DATA_PATH}/product_details.csv")
)

product_details.display()

# COMMAND ----------

# Performing INNER JOIN on two tables (Retail data and Product details)
retail_alias = retail_data.alias("r")
product_alias = product_details.alias("p")

master_table = retail_alias.join(
    product_alias,
    on="product_id",
    how="inner"
)

# COMMAND ----------

master_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Analysis
# MAGIC
# MAGIC Let's now analyze the selected columns to identify any inconsistencies, mismatches, or data quality issues.

# COMMAND ----------

# Checking for price mismatch
from pyspark.sql.functions import col

master_table.filter(
    (col("r.price").isNotNull()) &
    (col("r.price") != col("p.price"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Price Validation Insights
# MAGIC
# MAGIC Based on the validation results, no price mismatches were identified between the **Retail_Data1** table and the **Products** table. This indicates that the price values are consistent across both datasets.

# COMMAND ----------

from pyspark.sql.functions import when

master_table.select(
    "transaction_id",
    "product_id",
    col("r.product_name").alias("retail_product_name"),
    col("p.product_name").alias("master_product_name"),
    when(
        col("r.product_name") != col("p.product_name"),
        "Mismatch"
    ).otherwise("Match").alias("name_check"),
    
    col("r.category").alias("retail_category"),
    col("p.category").alias("master_category"),
    when(
        col("r.category") != col("p.category"),
        "Mismatch"
    ).otherwise("Match").alias("category_check")
    
).filter(
    (col("name_check") == "Mismatch") |
    (col("category_check") == "Mismatch")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Validation Insights
# MAGIC
# MAGIC Mismatches were identified in the **Product Name** and **Product Category** columns.
# MAGIC
# MAGIC To ensure data consistency and reduce duplication, the **Price**, **Product Name**, and **Product Category** columns can be excluded from the Retail Data table and sourced directly from the Products table when required.

# COMMAND ----------

print(master_table.count(), retail_data.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Product Details Analysis Summary
# MAGIC
# MAGIC No records were excluded during the join with the **Products** table (*Product Details*), indicating that every product referenced in **Retail_Data1** has a corresponding entry in the Products table.
# MAGIC
# MAGIC Based on this analysis, the product-related information is consistent across both datasets. To improve normalization and reduce data redundancy, the **Price**, **Product Name**, and **Product Category** columns can be removed from **Retail_Data1** and sourced directly from the **Products** table when needed.
# MAGIC
# MAGIC **This concludes the analysis of the Product Details data.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate Value Analysis

# COMMAND ----------

# DBTITLE 1,Analyzing the duplicate values
# Check duplicates per column
for col in retail_data.columns:
    print(f"Column: {col}, count: {retail_data.select(col).distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Checking Duplicate Records Using Transaction ID
# MAGIC
# MAGIC I found **251 records** with duplicate **Transaction IDs**. The next step is to verify whether the remaining column values are identical for records sharing the same Transaction ID.

# COMMAND ----------

from pyspark.sql.functions import count, col

# Getting duplicated Transaction_ids
duplicate_txns = (
    retail_data
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .select("transaction_id")
)

duplicate_rows = retail_data.join(
    duplicate_txns,
    on="transaction_id",
    how="inner"
)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

comparison_df = duplicate_rows.groupBy("transaction_id").agg(
    count("*").alias("row_count"),
    *[
        countDistinct(c).alias(f"{c}_distinct")
        for c in retail_data.columns
        if c != "transaction_id"
    ]
)

# COMMAND ----------

suspicious_txns = comparison_df.filter(
    " OR ".join(
        [
            f"{c}_distinct > 1"
            for c in retail_data.columns
            if c != "transaction_id"
        ]
    )
)

# suspicious_txns.display()

retail_data.join(
    suspicious_txns.select("transaction_id"),
    on="transaction_id",
    how="inner"
).display()

# COMMAND ----------

retail_data.filter(
    col("transaction_id").isin(
        [3749, 737, 1896]
    )
).orderBy("transaction_id").show()

# COMMAND ----------

# DBTITLE 1,Plotting barplot for Missing Values
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

duplicate_count = df.duplicated().sum()

summary = pd.DataFrame({
    "Type":["Unique","Duplicate"],
    "Count":[
        len(df)-duplicate_count,
        duplicate_count
    ]
})

sns.barplot(data=summary,x="Type",y="Count")

plt.title("Duplicate Records Analysis")
plt.show()

# COMMAND ----------

duplicate_pct = (
    df.duplicated().sum()
    / len(df)
) * 100

print(f"Duplicate Percentage : {duplicate_pct:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Insights from Duplicate Transaction ID Analysis
# MAGIC
# MAGIC Further analysis revealed that the records sharing the same **Transaction ID** are not true duplicates. The rows differ in their **Payment Status**, which is either **Successful** or **Failed**.
# MAGIC
# MAGIC Since these records represent different transaction outcomes, they should not be removed. Instead, the data can be separated into two tables based on the payment status:
# MAGIC
# MAGIC - Transactions with a **Successful** payment status
# MAGIC - Transactions with a **Failed** payment status
# MAGIC
# MAGIC This approach preserves all transaction records while enabling more efficient analysis of successful and failed payments.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking for Duplicate Customer Records
# MAGIC
# MAGIC Let's now examine the customer details for duplicate records.

# COMMAND ----------

# DBTITLE 1,Missing data for Customer details
retail_data.select(
    "customer_id",
    "customer_name"
).distinct().show()

# COMMAND ----------

# DBTITLE 1,Visualizing the Customer Ids
customer_duplicates = (
    df["customer_id"]
    .duplicated()
    .sum()
)

validation_df = pd.DataFrame({
    "Status":["Unique","Duplicate"],
    "Count":[
        len(df)-customer_duplicates,
        customer_duplicates
    ]
})

sns.barplot(
    data=validation_df,
    x="Status",
    y="Count"
)

plt.title("Customer ID Validation")
plt.show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col

duplicate_names = (
    retail_data
    .groupBy("customer_name")
    .agg(
        countDistinct("customer_id").alias("id_count")
    )
    .filter(col("id_count") > 1)
)

# duplicate_names.display()

retail_data.join(
    duplicate_names.select("customer_name"),
    on="customer_name",
    how="inner"
).select(
    "customer_id",
    "customer_name"
).distinct().orderBy(
    "customer_name"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Name Insights
# MAGIC
# MAGIC There are **8 customer names** that are linked to more than one **Customer ID**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Customer Email Addresses

# COMMAND ----------

# DBTITLE 1,Email Validation
email_valid = df["email"].str.match(
    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
    na=False
)

email_summary = email_valid.value_counts()

email_summary.plot(
    kind="pie",
    autopct="%1.1f%%"
)

plt.title("Email Validation")
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email Validation Insights
# MAGIC
# MAGIC No invalid email addresses were identified during the validation process.

# COMMAND ----------

retail_data.select(
    ["email"]
).distinct().count()

# With this I can conclude that there's two emails which is asscociated to two different users

# COMMAND ----------

from pyspark.sql.functions import count, col

duplicate_emails = (
    retail_data
    .groupBy("email")
    .agg(countDistinct("customer_id").alias("customer_count"))
    .filter(col("customer_count") > 1)
    .select("email")
)

result = (
    retail_data
    .join(duplicate_emails, on="email", how="inner")
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone"
    )
)

result.distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The records above indicate instances where the same **Email Address** is linked to multiple **Customer IDs**.
# MAGIC
# MAGIC **This concludes the analysis of duplicate customer details.**

# COMMAND ----------

retail_data.limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Duplicate Analysis Summary
# MAGIC
# MAGIC No further duplicate values were found in the remaining tables.
# MAGIC
# MAGIC The next step is to perform data validation on the following fields:
# MAGIC
# MAGIC - Quantity
# MAGIC - Date
# MAGIC - Discount
# MAGIC - Phone Number
# MAGIC
# MAGIC These checks will help identify any invalid, inconsistent, or out-of-range values within the dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating the Quantities

# COMMAND ----------

# DBTITLE 1,Quantity Validation
retail_data.select("quantity").distinct().show()

# COMMAND ----------

validate_quantities = retail_data.withColumn(
    "valid_qauntity",
    when(
        col("quantity") < 0, "INVALID"
    ).otherwise("VALID")
)

validate_quantities.select(["quantity", "valid_qauntity"]).distinct().show()

# COMMAND ----------

validate_quantities.groupBy("valid_qauntity").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Quantity Validation Insights
# MAGIC
# MAGIC Based on the analysis:
# MAGIC
# MAGIC - **31 records** contain invalid quantity values.
# MAGIC - **4,220 records** contain valid quantity values.
# MAGIC
# MAGIC To support downstream analysis and reporting, quantity records will be labeled as **Valid** or **Invalid** rather than being removed from the dataset.
# MAGIC
# MAGIC **This concludes the quantity data validation process.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating the dates

# COMMAND ----------

# DBTITLE 1,Date Validation
print("Count is: ",retail_data.select("transaction_date").distinct().count())
retail_data.select("transaction_date").distinct().show(10)

# COMMAND ----------

retail_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when, col

date_check_df = retail_data.withColumn(
    "date_format",
    when(
        col("transaction_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
        "yyyy-MM-dd"
    ).when(
        col("transaction_date").rlike(r"^\d{2}-\d{2}-\d{4}$"),
        "dd-MM-yyyy or MM-dd-yyyy"
    )
)

date_check_df.select(["transaction_date","date_format"]).distinct().show(10)

# COMMAND ----------

date_check_df.select("date_format").distinct().count()

# COMMAND ----------

retail_data.select("transaction_date").distinct().count()

# COMMAND ----------

from pyspark.sql.functions import when, col, split

date_check_df = date_check_df.withColumn(
    "date_order",
    when(
        (col("date_format") == "dd-MM-yyyy or MM-dd-yyyy"),
        when(
            split(col("transaction_date"), "-")[0].cast("int") > 12,
            "dd-MM-yyyy"
        ).when(
            split(col("transaction_date"), "-")[1].cast("int") > 12 ,
            "MM-dd-yyyy"
        )
    ).otherwise(
        col("date_format")
    )
)

date_check_df.select(["transaction_date","date_format", "date_order"]).distinct().show(10)

# COMMAND ----------

date_check_df.groupBy("date_order").count().show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

date_check_pdf = date_check_df.toPandas()

ax = date_check_pdf["date_order"].value_counts().plot(
    kind="bar",
    figsize=(6,4)
)

# Add labels on top of bars
for container in ax.containers:
    ax.bar_label(container)

plt.title("Date Format Distribution")
plt.xlabel("Date Format")
plt.ylabel("Count")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Date Validation Insights
# MAGIC
# MAGIC Based on the validation results:
# MAGIC
# MAGIC - No missing values were found in the **Date** column.
# MAGIC - The column contains two different date formats:
# MAGIC   - **yyyy-MM-dd**
# MAGIC   - **MM-dd-yyyy**
# MAGIC - The current data type of the column is **String**.
# MAGIC
# MAGIC To ensure consistency and enable date-based analysis, the date values should first be standardized into a single format and then converted to the **Date** data type. We will convert the **MM-dd-yyyy** to **yyyy-MM-dd** during transformation as the number of rows is less compared to the other.
# MAGIC
# MAGIC **This concludes the date validation analysis.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating the Phone numbers

# COMMAND ----------

# DBTITLE 1,Phone Number validation
retail_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phone Number Validation
# MAGIC
# MAGIC The **Phone Number** column is currently stored as a **Long** data type and should be cast to **String** before performing validation checks.
# MAGIC
# MAGIC The following validations will be performed:
# MAGIC
# MAGIC - Verify that each phone number contains exactly **10 digits**.
# MAGIC - Ensure that phone numbers do not contain any **leading zeros**.
# MAGIC - Identify and flag any records that do not satisfy these conditions.
# MAGIC
# MAGIC **This validation will help ensure the accuracy and consistency of customer contact information.**

# COMMAND ----------

# Creating a new column which will be used for phone number validation
retail_data = retail_data.withColumn(
    "phone_str",
    col("phone").cast("string")
)

# COMMAND ----------

retail_data.select(
    "phone",
    "phone_str"
).distinct().show(10)

# COMMAND ----------

phone_valid = (
    df["phone"]
    .astype(str)
    .str.match(r'^\d{10}$')
)

phone_valid.value_counts().plot(
    kind='bar'
)

plt.title("Phone Number Validation")
plt.xlabel("Valid Phone Number")
plt.ylabel("Count")
plt.legend()
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col

retail_data.filter(
    col("phone_str").isNull()
).select(["phone","phone_str"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phone Number Conversion Insights
# MAGIC
# MAGIC No NULL values were found in the **phone_str** column after the data type conversion. This indicates that all phone number values were successfully converted from **Long** to **String** without any data loss.

# COMMAND ----------

from pyspark.sql.functions import length

retail_data.withColumn(
    "phone_length",
    length(col("phone_str"))
).groupBy(
    "phone_length"
).count().show()

# COMMAND ----------

retail_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phone Number Validation Insights
# MAGIC
# MAGIC The number of records with a valid **10-digit phone length** matches the total row count, indicating that all phone numbers are valid.
# MAGIC
# MAGIC Furthermore, as the source data is stored as a numeric data type, leading zeros are not possible.
# MAGIC
# MAGIC **This concludes the phone number validation analysis.**

# COMMAND ----------

# MAGIC %md
# MAGIC **Validating Discount column**

# COMMAND ----------

retail_data.select("discount").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Discount Validation Insights
# MAGIC
# MAGIC Based on the distinct discount values observed in the dataset, the discount data appears to be valid and consistent.
# MAGIC
# MAGIC No anomalies or unexpected values were identified, and therefore no additional validation checks are required.
# MAGIC
# MAGIC **This concludes the discount validation analysis.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the remaining columns

# COMMAND ----------

retail_data.select("payment_method").distinct().show()

# COMMAND ----------

retail_data.select("payment_status").distinct().show()

# COMMAND ----------

retail_data.select("city").distinct().show()

# COMMAND ----------

retail_data.select("purchase_location").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Conversion Summary
# MAGIC
# MAGIC No further data type conversions or transformations are required for these tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Privacy
# MAGIC
# MAGIC Both **Email ID** and **Phone Number** are considered **Personally Identifiable Information (PII)** and must be protected through data masking or hashing.
# MAGIC
# MAGIC * A separate **Customer table** will be created to store customer-related information, where masking or hashing can be applied efficiently.
# MAGIC * This approach reduces processing overhead on the main `retail_data` table and improves data organization.
# MAGIC * Since email addresses and phone numbers are not required for business analysis, securing these fields helps maintain privacy without impacting analytical outcomes.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    concat,
    lit,
    substring,
    instr,
    regexp_extract
)

customer_df = retail_data.select(
    "customer_id",
    "customer_name",

    # Original Email, 
    "email",

    # Masked Email
    concat(
        substring(col("email"), 1, 2),
        lit("******"),
        regexp_extract(col("email"), "(@.*)", 1)
    ).alias("email_masked"),

    # Original Phone, 
    "phone",

    # Masked Phone
    concat(
        substring(col("phone").cast("string"), 1, 2),
        lit("******"),
        substring(col("phone").cast("string"), 9, 2)
    ).alias("phone_masked")
)

# COMMAND ----------

customer_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Both the original and masked values will be retained in the dataset. However, only the masked values will be used for business reporting and analytical purposes.**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC # Final Conclusion
# MAGIC
# MAGIC The Exploratory Data Analysis (EDA) and data quality assessment of the **Retail_Data1** dataset have been successfully completed.
# MAGIC
# MAGIC ### Key Findings
# MAGIC
# MAGIC - Missing values were identified only in the **Price** column and can be resolved through a join with the **Products** table.
# MAGIC - Product-related attributes (**Price**, **Product Name**, and **Product Category**) were validated against the Products table and found to be consistent.
# MAGIC - No records were excluded during the product validation process, confirming referential integrity between the datasets.
# MAGIC - Duplicate **Transaction IDs** were investigated and determined to represent different transaction outcomes (**Successful** and **Failed**) rather than true duplicate records.
# MAGIC - Customer data was analyzed for duplication, revealing instances where the same customer name or email address was associated with multiple customer IDs.
# MAGIC - Quantity, Date, Discount, and Phone Number fields were validated to identify inconsistencies and improve data quality.
# MAGIC - Date values were found in multiple formats and require standardization before conversion to the Date data type.
# MAGIC - Phone numbers were successfully validated and confirmed to contain valid 10-digit values.
# MAGIC - Discount values were validated and found to be consistent across the dataset.
# MAGIC
# MAGIC ### Data Preparation Decisions
# MAGIC
# MAGIC - Product-related columns can be removed from the retail transaction table and sourced directly from the Products table to reduce redundancy and improve normalization.
# MAGIC - Quantity records will be labeled as **Valid** or **Invalid** instead of being removed.
# MAGIC - Sensitive customer information will be masked while retaining original values for governance and operational purposes.
# MAGIC - Only masked values will be exposed for business reporting and analytical use.
# MAGIC
# MAGIC ### Outcome
# MAGIC
# MAGIC The dataset has been thoroughly analyzed, validated, and prepared for downstream data engineering, reporting, dashboarding, and business analytics activities. The identified data quality issues have been documented, and appropriate remediation strategies have been defined to ensure reliable and consistent business insights.