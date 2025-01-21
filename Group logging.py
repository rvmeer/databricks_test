# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `experiments3`.`logging`.`combined`;

# COMMAND ----------

# Display the min and max date from the _time column
max_dates = spark.sql("SELECT MIN(iso_timestamp) as min_time, MAX(iso_timestamp) as max_time FROM _sqldf")

display(max_dates)

# COMMAND ----------

# Only retrain where cpd_name is not NULL
df_filtered = spark.sql("SELECT * FROM _sqldf WHERE cpd_name IS NOT NULL")

# COMMAND ----------

# From the filtered df, show the unique cidt_version, user and cpd_name with the count of events
df_grouped = df_filtered.groupBy("cidt_version", "user", "cpd_name").count()

df_grouped.show(1000,False)


# COMMAND ----------

# Write the grouped df to table in the experiments3 catalog, make persistent
df_grouped.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped")



# COMMAND ----------

# From the filtered df, where feature is not the string 'NULL', group by user, cpd_name and feature and count the events
df_grouped_with_feature = df_filtered.where("feature <> 'NULL'").groupBy("user", "cpd_name", "feature").count()

df_grouped_with_feature.show(1000,False)

# COMMAND ----------

df_grouped_with_feature.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped_with_feature")

