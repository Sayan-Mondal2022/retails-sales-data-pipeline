# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformation Pipeline
# MAGIC
# MAGIC ### Objectives
# MAGIC
# MAGIC - Load Product Details, Retail Data 1, and Retail Data 2.
# MAGIC - Create a unified **Customers** table by combining customer data from both retail datasets.
# MAGIC - Apply PII masking to protect customer privacy.
# MAGIC - Create a consolidated **Sales** table containing:
# MAGIC   * Transaction details
# MAGIC   * Product ID
# MAGIC   * Customer ID
# MAGIC   * Other transaction-related attributes
# MAGIC - Remove all customer and product descriptive details from the Sales table.
# MAGIC - Add a column to classify quantities as **Valid** or **Invalid**.
# MAGIC - Standardize all dates to the format **yyyy-MM-dd**.
# MAGIC - Generate the final three tables:
# MAGIC   * **Products**
# MAGIC   * **Customers**
# MAGIC   * **Sales**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Loading the Datasets
# Setting up the Path

# Set the storage account key in spark config
spark.conf.set(
    "fs.azure.account.key.retailsalesresources.blob.core.windows.net",
    dbutils.secrets.get("databricksScope", "storageaccount-secrets")  
)

# File path for raw-data container
RAW_DATA_PATH = "wasbs://raw-data@retailsalesresources.blob.core.windows.net"
TRANSFORMED_DATA_PATH = "wasbs://transformed-data@retailsalesresources.blob.core.windows.net"


try:
    # Loading the CSV files
    retail_data1 = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_DATA_PATH}/retail_data.csv")
    )

    retail_data2 = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_DATA_PATH}/retail_data2.csv")
    )

    product_details = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_DATA_PATH}/product_details.csv")
    )

    print("Datasets have been loaded successfuly!!")
except Exception as e:
    raise Exception("Couldn't load the CSV file")


# COMMAND ----------

# DBTITLE 1,Checking for successful load
EXPECTED_PRODUCT_DETAILS_COLS = 4
EXPECTED_RETAIL_DATA_COLS = 16

if product_details.count() == 0 or len(product_details.columns) != EXPECTED_PRODUCT_DETAILS_COLS:
    raise Exception("Failed to load Product details Table!")
print("Product details loaded successfully")

if retail_data1.count() == 0 or len(retail_data1.columns) != EXPECTED_RETAIL_DATA_COLS:
    raise Exception("Failed to load Retail data Table!")
print("Retail Data1 loaded successfully")

if retail_data2.count() == 0 or len(retail_data2.columns) != EXPECTED_RETAIL_DATA_COLS:
    raise Exception("Failed to load Retail data Table!")
print("Retail Data2 loaded successfully")

# COMMAND ----------

# DBTITLE 1,Displaying Retail Data1
print("RETAIL DATA1\n")

print("Rows loaded: ", retail_data1.count())
retail_data1.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Displaying Retail Data2
print("RETAIL DATA2\n")

print("Rows loaded: ", retail_data2.count())
retail_data2.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Displaying Product Details
print("PRODUCT DETAILS")

print("Rows loaded: ", product_details.count())
product_details.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transforming the Product Details table

# COMMAND ----------

# DBTITLE 1,Importing all the dependencies
from pyspark.sql.functions import (
    col, 
    isnan,
    split, 
    when, 
    to_date, 
    sha2, 
    length, 
    trim,
    regexp_replace,
    initcap
)

# COMMAND ----------

# DBTITLE 1,Creating products table
products = (
    product_details
    .filter(
        col("product_id").isNotNull() &
        col("product_name").isNotNull() &
        col("category").isNotNull() &
        col("price").isNotNull() &
        ~isnan(col("price"))
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "price"
    )
    .distinct()
)

if (
    products.count() == 0
    or products.count() != product_details.count()
    or len(products.columns) != EXPECTED_PRODUCT_DETAILS_COLS
):
    raise Exception("Failed to extract the Product details")

print("Products table is created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating customers table:
# MAGIC
# MAGIC - Extract the data from both the tables, Retail data1 and data2
# MAGIC - Then exclude all the duplicates
# MAGIC - Then perform Hashing on the Password and Phone Number
# MAGIC - Then store the final cleaned customer data in Customers.csv

# COMMAND ----------

# DBTITLE 1,Extracting customer raw data from Retail data1 and data2
# 1. Extracting the Raw data from the Retail data1 and data2

# REGEX for Email validation
email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


customers_data1 = (
    retail_data1
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone"
    )
    .filter(
        col("customer_id").isNotNull() &
        col("customer_name").isNotNull() &
        col("email").isNotNull() &
        col("phone").isNotNull() &

        (trim(col("customer_id")) != "") &
        (trim(col("customer_name")) != "") &
        (trim(col("email")) != "") &

        col("email").rlike(email_regex) &
        (length(regexp_replace(col("phone").cast("string"), r"\D", "")) == 10)
    )
    .distinct()
)

customers_data2 = (
    retail_data2
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone"
    )
    .filter(
        col("customer_id").isNotNull() &
        col("customer_name").isNotNull() &
        col("email").isNotNull() &
        col("phone").isNotNull() &

        (trim(col("customer_id")) != "") &
        (trim(col("customer_name")) != "") &
        (trim(col("email")) != "") &

        col("email").rlike(email_regex) &
        (length(regexp_replace(col("phone").cast("string"), r"\D", "")) == 10)
    )
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Combining both customer tables from retail data1 and data2
if customers_data1.count() == 0 or customers_data2.count() == 0:
    raise Exception("Failed to extract the Customer data")

# 2. Making a customers_raw table to store all the raw data
customers_raw = customers_data1.unionByName(customers_data2)

# ERROR: If the Master table failed to union both the customers table
if customers_raw.count() == 0:
    raise Exception("Failed to union the Customer data into master table")

# ERROR: When the row count doesn't match, Means there's data loss
if customers_raw.count() != (customers_data1.count() + customers_data2.count()):
    raise Exception("Customers data not loaded successfully!")

print("Successfully extracted and loaded data into Master Table")

# COMMAND ----------

# DBTITLE 1,Hashing email and phone number
# 3. Hashing the password and Email
customers_clean = (
    customers_raw
    .withColumn("email_hash", sha2(col("email"), 256))
    .withColumn("phone_hash", sha2(col("phone").cast("string"), 256))
)

if customers_clean.count() == 0 or customers_clean.count() != customers_raw.count():
    raise Exception("Data not cleaned properly")

print("Hashing Completed!")

# COMMAND ----------

# DBTITLE 1,creating cleaned customers table
customers = customers_clean.select(["customer_id","customer_name", "email_hash", "phone_hash"])

if customers.count() == 0 or customers_clean.count() != customers.count():
    raise Exception("Data not cleaned properly")

print("Customers Table is formed successfully")

customers.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Customers table is fomration is completed**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now making a Master retail table named as sales

# COMMAND ----------

# DBTITLE 1,Loading Raw Sales data
# Loading retail data1
sales_data1 = (
    retail_data1
    .filter(
        col("transaction_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &

        col("quantity").isNotNull() &
        col("discount").isNotNull() &
        col("transaction_date").isNotNull() &

        col("city").isNotNull() &
        col("purchase_location").isNotNull() &
        col("payment_method").isNotNull() &
        col("payment_status").isNotNull() &

        (trim(col("city")) != "") &
        (trim(col("purchase_location")) != "") &
        (trim(col("payment_method")) != "") &
        (trim(col("payment_status")) != "") &
        (trim(col("transaction_date")) != "")
    )
    .select(
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "city",
        "transaction_date",
        col("purchase_location").alias("purchase_mode"),
        "payment_method",
        "discount",
        "payment_status"
    )
    .distinct()
)

sales_data2 = (
    retail_data2
    .filter(
        col("transaction_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &

        col("quantity").isNotNull() &
        col("discount").isNotNull() &
        col("transaction_date").isNotNull() &

        col("city").isNotNull() &
        col("purchase_location").isNotNull() &
        col("payment_method").isNotNull() &
        col("payment_status").isNotNull() &

        (trim(col("city")) != "") &
        (trim(col("purchase_location")) != "") &
        (trim(col("payment_method")) != "") &
        (trim(col("payment_status")) != "") &
        (trim(col("transaction_date")) != "")
    )
    .select(
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "city",
        "transaction_date",
        col("purchase_location").alias("purchase_mode"),
        "payment_method",
        "discount",
        "payment_status"
    )
    .distinct()
)



# Checking if the data is loaded properly
# ERROR: sales_data1 or sales_data2 is empty
if sales_data1.count() == 0 or sales_data2.count() == 0:
    raise Exception("Failed to extract the Sales data")

sales_raw_data = sales_data1.unionByName(sales_data2)


# Checking for Data load
# ERROR: sales_raw_data is empty
if sales_raw_data.count() != sales_data1.count() + sales_data2.count():
    raise Exception("Data not loaded properly")
print("Sales Data is formed successfully")

# COMMAND ----------

# DBTITLE 1,Adding valid_quantity and valid_discount
sales_raw_data = sales_raw_data.withColumn(
    "valid_quantity",
    when(col("quantity") >= 0, "Valid").otherwise("Invalid")
)
sales_raw_data = sales_raw_data.withColumn(
    "valid_discount",
    when(col("discount") >= 0, "Valid").otherwise("Invalid")
)

# COMMAND ----------

# DBTITLE 1,Displaying valid_quantity and valid_discount
sales_raw_data.groupBy("valid_quantity").count().display()
sales_raw_data.groupBy("valid_discount").count().display()

# COMMAND ----------

# DBTITLE 1,Formatting Transaction date
sales_raw_data = sales_raw_data.withColumn(
    "formatted_date",
    when(
        split(col("transaction_date"), "-").getItem(0).cast("int") > 31,
        to_date(col("transaction_date"), "yyyy-MM-dd")
    ).when(
        split(col("transaction_date"), "-").getItem(0).cast("int") > 12,
        to_date(col("transaction_date"), "dd-MM-yyyy")
    ).otherwise(
        to_date(col("transaction_date"), "MM-dd-yyyy")
    )
)

if sales_raw_data.count() != sales_data1.count() + sales_data2.count():
    raise Exception("Data not loaded properly")
print("Date is Formatted Successfully successfully")

# COMMAND ----------

# DBTITLE 1,Checking whether all the columns are present or not
EXPECTED_COLS = 13

if len(sales_raw_data.columns) != EXPECTED_COLS:
    raise Exception("Failed to create Master sales raw data table!")

print("Sales Raw Data is formed successfully!!")

# COMMAND ----------

# DBTITLE 1,Displaying Sales Raw Data Table
sales_raw_data.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Creating final sales table
sales =  sales_raw_data.select(
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "valid_quantity",
    "city",
    col("formatted_date").alias("transaction_date"),
    "purchase_mode",
    "payment_method",
    "discount",
    "valid_discount",
    "payment_status"
)

sales = sales.withColumn(
    "payment_status",
    initcap(col("payment_status"))
)

if sales.count() == 0 or sales.count() != sales_raw_data.count() or (len(sales.columns)) != 12:
    raise Exception("Failed to create Sales table!")
print("Successfully created Sales Table!")

# COMMAND ----------

# DBTITLE 1,Displaying the sales table
sales.display()

# COMMAND ----------

# DBTITLE 1,Saving the data into Transformed-data container
# Save Customer data
customers.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{TRANSFORMED_DATA_PATH}/dim_customer")

print("Customers data has been saved")

# Save Product data
products.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{TRANSFORMED_DATA_PATH}/dim_product")

print("Products data has been saved")

# Save Sales data
sales.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{TRANSFORMED_DATA_PATH}/fact_sales")

print("Sales data has been saved")

# COMMAND ----------

# DBTITLE 1,Success Message
dbutils.notebook.exit(
    "SUCCESS: Customer Dimension created successfully"
)