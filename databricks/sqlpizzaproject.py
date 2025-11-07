# Databricks notebook source
#dbutils.fs.mount(
    #source = "wasbs://container@storgae.blob.core.windows.net",
    #mount_point "/mnt/databricks",
    #extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>",key = #"<key-name>")})


# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://raw-input@sqlpizzaprojectstorage.blob.core.windows.net",
    mount_point = "/mnt/raw-input",
    extra_configs = {"fs.azure.account.key.sqlpizzaprojectstorage.blob.core.windows.net":"L+HKtm5XYDUdFYiV9h1bn8UanpJAh84Gcvb+XpSXdLhdC20tv1Mvyb8UbTbnBXZFaIz+BeYZfoBX+AStCGFirw=="}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw-input")

# COMMAND ----------

df = spark.read.format("csv")\
    .options(header = True)\
        .option("inferSchema",True)\
            .load("/mnt/raw-input/dbo.pizza_sales.txt")
display(df)

# COMMAND ----------

df.createOrReplaceTempView("df_sales_analysis")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_sales_analysis
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_id,
# MAGIC quantity,
# MAGIC date_format(order_date,'MMM') as month_name,
# MAGIC date_format(order_date,'EEEE') as day_name,
# MAGIC hour(order_time) as order_hour,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from df_sales_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregated Table 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(order_id) as order_id,
# MAGIC sum(quantity) as quantity,
# MAGIC date_format(order_date,'MMM') as month_name,
# MAGIC date_format(order_date,'EEEE') as day_name,
# MAGIC hour(order_time) as order_time,
# MAGIC sum(unit_price) as unit_price,
# MAGIC sum(total_price) as total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from df_sales_analysis
# MAGIC group by 3,4,5,8,9,10

# COMMAND ----------

