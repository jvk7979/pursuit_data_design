# Setup Instructions for Databricks
# 1. Upload the CSV files to the Databricks workspace.
# 2. Ensure that the Spark cluster is running.
# 3. Execute this script in a Databricks notebook.
# 4. Validate that tables are created in Delta format.
# 5. Run the SQL queries to verify the output.


# Setup Instructions for Jupyter Notebook:
# 1. Install required dependencies:
#    !pip install pyspark delta-spark
# 2. Start a Spark session in Jupyter Notebook:
#    from pyspark.sql import SparkSession
#    spark = SparkSession.builder.appName("pursuit_Pipeline").getOrCreate()
# 3. Ensure input CSV files are stored in /mnt/data/ or in local
# 4. Run the notebook sequentially to load, clean, and store the data in Delta format.
# 5. Query the Delta tables using SQL for further analysis.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session (if needed, typically done outside this script)
# spark = SparkSession.builder.appName("CustomerDataProcessing").getOrCreate()

# File Paths for input CSV files stored in mounted storage
contacts_path = "/mnt/data/contacts.csv"
places_path = "/mnt/data/places.csv"
customer_mapping_path = "/mnt/data/customer_mapping.csv"
tech_stack_path = "/mnt/data/tech_stack.csv"
customer_path = "/mnt/data/customer.csv"
customer_contact_relation_path = "/mnt/data/customer_contact_relation.csv"
customer_place_relation_path = "/mnt/data/customer_place_relation.csv"

# Function to read CSV files into Spark DataFrames
def read_csv(file_path):
    return spark.read.option("header", "true").option("inferschema", "true").csv(file_path)

# Load CSV data into DataFrames
contacts_df = read_csv(contacts_path)
places_df = read_csv(places_path)
customer_mapping_df = read_csv(customer_mapping_path)
technology_df = read_csv(tech_stack_path)
customer_df = read_csv(customer_path)
customer_contact_relation_df = read_csv(customer_contact_relation_path)
customer_place_relation_df = read_csv(customer_place_relation_path)

# Data Cleaning & Transformation
contacts_df = (
    contacts_df
    .withColumn("emails", F.lower(F.col("emails")))  # Convert email to lowercase for consistency
    .withColumn("phone", F.regexp_extract(F.col("phone"), r"(\(\d{3}\) \d{3}-\d{4})", 1))  # Extract only the main phone number
    .withColumn("extension", F.regexp_extract(F.col("phone"), r"(ext\. \d+)", 1))  # Extract phone number extension into a new column
    .withColumn("timestamp", F.to_timestamp(F.col("created_at"), "EEE MMM dd yyyy HH:mm:ss 'GMT'Z"))  # Convert 'created_at' to timestamp format
)

# Function to write DataFrame to Delta tables with overwrite mode
def write_to_delta(df, table_name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# Write cleaned data to Delta tables in the "pursuit" database
write_to_delta(contacts_df, "pursuit.contacts")
write_to_delta(places_df, "pursuit.places")
write_to_delta(customer_mapping_df, "pursuit.customer_mapping")
write_to_delta(technology_df, "pursuit.tech_stack")
write_to_delta(customer_df, "pursuit.customer")
write_to_delta(customer_contact_relation_df, "pursuit.customer_contact_relation")
write_to_delta(customer_place_relation_df, "pursuit.customer_place_relation")




