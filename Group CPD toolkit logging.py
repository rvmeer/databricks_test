# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `experiments3`.`logging`.`combined` where not classic and cpd_name is not NULL;

# COMMAND ----------

from pyspark.sql.functions import col, date_diff

df = _sqldf
df.createOrReplaceTempView("df") # Make sure we can use it in textual queries

# select min(iso_timestamp) as min_timestamp, max(iso_timestamp) as max_timestamp from df
time_window = spark.sql("select min(iso_timestamp) as min_time, max(iso_timestamp) as max_time from df")

# Add a column days to the time window
time_window = time_window.withColumn("days", date_diff(col("max_time"), col("min_time")) + 1)

display(time_window)

# And wtier to table
time_window.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.time_window")

# COMMAND ----------

# From the filtered df, show the unique cidt_version, user and cpd_name with the count of events
df_grouped_by_version = df.groupBy("cidt_version", "user", "cpd_name").count()

df_grouped_by_version.show(1000,False)


# COMMAND ----------

# Write the grouped df to table in the experiments3 catalog, make persistent
df_grouped_by_version.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped_by_version")



# COMMAND ----------

# From the filtered df, where feature is not the string 'NULL', group by user, cpd_name and feature and count the events
df_grouped_by_feature = df.where("feature <> 'NULL'").groupBy("user", "cpd_name", "feature").count()

df_grouped_by_feature.show(1000,False)

# COMMAND ----------

df_grouped_by_feature.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped_by_feature")


# COMMAND ----------

from pyspark.sql.functions import collect_list, sort_array, concat_ws

df_grouped_by_platform = df.groupBy("user", "cpd_name", "platform").count()

# Remove the count column
df_grouped_by_platform = df_grouped_by_platform.drop("count")

# Order by user
df_grouped_by_platform = df_grouped_by_platform.orderBy("user")

df_grouped_by_platform = (df_grouped_by_platform
    .groupBy('user', 'platform')
    .agg(concat_ws(',', sort_array(collect_list('cpd_name'))).alias('cpd_names'))
)

df_grouped_by_platform.show(1000,False)

# COMMAND ----------

df_grouped_by_platform.write.format("delta").mode("overwrite").saveAsTable("experiments3.logging.grouped_by_platform")
