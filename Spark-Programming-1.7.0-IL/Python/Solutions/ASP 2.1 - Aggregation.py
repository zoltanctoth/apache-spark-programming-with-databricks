# Databricks notebook source

# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Group data by specified columns
# MAGIC 1. Apply grouped data methods to aggregate data
# MAGIC 1. Apply built-in functions to aggregate data
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `groupBy`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData" target="_blank" target="_blank">Grouped Data</a>: `agg`, `avg`, `count`, `max`, `sum`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `approx_count_distinct`, `avg`, `sum`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the BedBricks events dataset.

# COMMAND ----------

df = spark.read.parquet(eventsPath)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grouping data
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy
# MAGIC Use the DataFrame `groupBy` method to create a grouped data object. 
# MAGIC 
# MAGIC This grouped data object is called `RelationalGroupedDataset` in Scala and `GroupedData` in Python.

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.html" target="_blank">GroupedData</a> object.
# MAGIC 
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

eventCountsDF = df.groupBy("event_name").count()
display(eventCountsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we're getting the average purchase revenue for each.

# COMMAND ----------

avgStatePurchasesDF = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avgStatePurchasesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC And here the total quantity and sum of the purchase revenue for each combination of state and city.

# COMMAND ----------

cityPurchaseQuantitiesDF = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(cityPurchaseQuantitiesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC 
# MAGIC In Scala, this is <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_bank">`org.apache.spark.sql.functions`</a>, and <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">`pyspark.sql.functions`</a> in Python. Functions from this module must be imported into your code.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate Functions
# MAGIC 
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC 
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">`agg`</a> to apply built-in aggregate functions
# MAGIC 
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">`alias`</a>.

# COMMAND ----------

from pyspark.sql.functions import sum

statePurchasesDF = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(statePurchasesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply multiple aggregate functions on grouped data

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

stateAggregatesDF = (df
                     .groupBy("geo.state")
                     .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                          approx_count_distinct("user_id").alias("distinct_users"))
                    )

display(stateAggregatesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(
    spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
    .withColumn("sqrt", sqrt("id"))
    .withColumn("cos", cos("id"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue by Traffic Lab
# MAGIC Get the 3 traffic sources generating the highest total revenue.
# MAGIC 1. Aggregate revenue by traffic source
# MAGIC 2. Get top 3 traffic sources by total revenue
# MAGIC 3. Clean revenue columns to have two decimal places
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: groupBy, sort, limit
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html?highlight=column#pyspark.sql.Column" target="_blank">Column</a>: alias, desc, cast, operators
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-in Functions</a>: avg, sum

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame **`df`**.

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.read.parquet(eventsPath)
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Aggregate revenue by traffic source
# MAGIC - Group by **`traffic_source`**
# MAGIC - Get sum of **`revenue`** as **`total_rev`**
# MAGIC - Get average of **`revenue`** as **`avg_rev`**
# MAGIC 
# MAGIC Remember to import any necessary built-in functions.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import avg, col, sum

trafficDF = (df
             .groupBy("traffic_source")
             .agg(sum(col("revenue")).alias("total_rev"),
                  avg(col("revenue")).alias("avg_rev"))
            )

display(trafficDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(12704560.0, 1083.175), (78800000.3, 983.2915), (24797837.0, 1076.6221), (47218429.0, 1086.8303), (16177893.0, 1083.4378), (8044326.0, 1087.218)]
testDF = trafficDF.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected1 == result1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get top three traffic sources by total revenue
# MAGIC - Sort by **`total_rev`** in descending order
# MAGIC - Limit to first three rows

# COMMAND ----------

# ANSWER
topTrafficDF = trafficDF.sort(col("total_rev").desc()).limit(3)
display(topTrafficDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

expected2 = [(78800000.3, 983.2915), (47218429.0, 1086.8303), (24797837.0, 1076.6221)]
testDF = topTrafficDF.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected2 == result2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Limit revenue columns to two decimal places
# MAGIC - Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
# MAGIC   - Use **`withColumn()`** with the same names to replace these columns
# MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

# COMMAND ----------

# ANSWER
finalDF = (topTrafficDF
           .withColumn("avg_rev", (col("avg_rev") * 100).cast("long") / 100)
           .withColumn("total_rev", (col("total_rev") * 100).cast("long") / 100)
          )

display(finalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

expected3 = [(78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result3 = [(row.total_rev, row.avg_rev) for row in finalDF.collect()]

assert(expected3 == result3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Bonus: Rewrite using a built-in math function
# MAGIC Find a built-in math function that rounds to a specified number of decimal places

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import round

bonusDF = (topTrafficDF
           .withColumn("avg_rev", round("avg_rev", 2))
           .withColumn("total_rev", round("total_rev", 2))
          )

display(bonusDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

expected4 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result4 = [(row.total_rev, row.avg_rev) for row in bonusDF.collect()]

assert(expected4 == result4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Chain all the steps above

# COMMAND ----------

# ANSWER
chainDF = (df
           .groupBy("traffic_source")
           .agg(sum(col("revenue")).alias("total_rev"),
                avg(col("revenue")).alias("avg_rev"))
           .sort(col("total_rev").desc())
           .limit(3)
           .withColumn("avg_rev", round("avg_rev", 2))
           .withColumn("total_rev", round("total_rev", 2))
          )

display(chainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

expected5 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result5 = [(row.total_rev, row.avg_rev) for row in chainDF.collect()]

assert(expected5 == result5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
