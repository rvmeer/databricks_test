# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `experiments3`.`logging`.`cidt_lite_logging_last_24_hours_2025_01_20`;

# COMMAND ----------

# Display the min and max date from the _time column
max_dates = spark.sql("SELECT MIN(_time) as min_time, MAX(_time) as max_time FROM _sqldf")

display(max_dates)

# COMMAND ----------

# Only retrain where cpd_name is not NULL
df_filtered = spark.sql("SELECT * FROM _sqldf WHERE cpd_name IS NOT NULL")

# COMMAND ----------

# From the filtered df, show the unique cidt_version, user and cpd_name with the count of events
df_grouped = df_filtered.groupBy("cidt_version", "user", "cpd_name").count()

df_grouped.show()


# COMMAND ----------

# Write the grouped df to table in the experiments3 catalog, make persistent
df_grouped.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped")



# COMMAND ----------

# Now read the grouped table back in
df_grouped_delta = spark.read.format("delta").load("/tmp/delta/grouped")

df_grouped_delta.show()
