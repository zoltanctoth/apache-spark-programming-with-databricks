// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # DataFrame & Column
// MAGIC ##### Objectives
// MAGIC 1. Construct columns
// MAGIC 1. Subset columns
// MAGIC 1. Add or replace columns
// MAGIC 1. Subset rows
// MAGIC 1. Sort rows
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `select`, `selectExpr`, `drop`, `withColumn`, `withColumnRenamed`, `filter`, `distinct`, `limit`, `sort`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a>: `alias`, `isin`, `cast`, `isNotNull`, `desc`, operators

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use the BedBricks events dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Column Expressions
// MAGIC 
// MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
// MAGIC 
// MAGIC Construct a new Column based on existing columns in a DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC Scala supports an additional syntax for creating a new Column based on existing columns in a DataFrame

// COMMAND ----------

$"device"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Column Operators and Methods
// MAGIC | Method | Description |
// MAGIC | --- | --- |
// MAGIC | \*, + , <, >= | Math and comparison operators |
// MAGIC | ==, != | Equality and inequality tests (Scala operators are `===` and `=!=`) |
// MAGIC | alias | Gives the column an alias |
// MAGIC | cast, astype | Casts the column to a different data type |
// MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
// MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

// COMMAND ----------

// MAGIC %md
// MAGIC Create complex expressions with existing columns, operators, and methods.

// COMMAND ----------

// MAGIC %md
// MAGIC Here's an example of using these column expressions in the context of a DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrame Transformation Methods
// MAGIC | Method | Description |
// MAGIC | --- | --- |
// MAGIC | select | Returns a new DataFrame by computing given expression for each element |
// MAGIC | drop | Returns a new DataFrame with a column dropped |
// MAGIC | withColumnRenamed | Returns a new DataFrame with a column renamed |
// MAGIC | withColumn | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
// MAGIC | filter, where | Filters rows using the given condition |
// MAGIC | sort, orderBy | Returns a new DataFrame sorted by the given expressions |
// MAGIC | dropDuplicates, distinct | Returns a new DataFrame with duplicate rows removed |
// MAGIC | limit | Returns a new DataFrame by taking the first n rows |
// MAGIC | groupBy | Groups the DataFrame using the specified columns, so we can run aggregation on them |

// COMMAND ----------

// MAGIC %md
// MAGIC ### Subset columns
// MAGIC Use DataFrame transformations to subset columns

// COMMAND ----------

// MAGIC %md
// MAGIC #### `select()`
// MAGIC Selects a list of columns or column based expressions

// COMMAND ----------

// MAGIC %md
// MAGIC #### `selectExpr()`
// MAGIC Selects a list of SQL expressions

// COMMAND ----------

// MAGIC %md
// MAGIC #### `drop()`
// MAGIC Returns a new DataFrame after dropping the given column, specified as a string or Column object
// MAGIC 
// MAGIC Use strings to specify multiple columns

// COMMAND ----------

// MAGIC %md
// MAGIC ### Add or replace columns
// MAGIC Use DataFrame transformations to add or replace columns

// COMMAND ----------

// MAGIC %md
// MAGIC #### `withColumn()`
// MAGIC Returns a new DataFrame by adding a column or replacing an existing column that has the same name.

// COMMAND ----------

// MAGIC %md
// MAGIC #### `withColumnRenamed()`
// MAGIC Returns a new DataFrame with a column renamed.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Subset Rows
// MAGIC Use DataFrame transformations to subset rows

// COMMAND ----------

// MAGIC %md
// MAGIC #### `filter()`
// MAGIC Filters rows using the given SQL expression or column based condition.

// COMMAND ----------

// MAGIC %md
// MAGIC #### `dropDuplicates()`
// MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
// MAGIC 
// MAGIC ##### Alias: `distinct`

// COMMAND ----------

// MAGIC %md
// MAGIC #### `limit()`
// MAGIC Returns a new DataFrame by taking the first n rows.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Sort rows
// MAGIC Use DataFrame transformations to sort rows

// COMMAND ----------

// MAGIC %md
// MAGIC #### `sort()`
// MAGIC Returns a new DataFrame sorted by the given columns or expressions.
// MAGIC 
// MAGIC ##### Alias: `orderBy`

// COMMAND ----------

// MAGIC %md
// MAGIC # Purchase Revenues Lab
// MAGIC 
// MAGIC Prepare dataset of events with purchase revenue.
// MAGIC 
// MAGIC ##### Tasks
// MAGIC 1. Extract purchase revenue for each event
// MAGIC 2. Filter events where revenue is not null
// MAGIC 3. Check what types of events have revenue
// MAGIC 4. Drop unneeded column
// MAGIC 
// MAGIC ##### Methods
// MAGIC - DataFrame: `select`, `drop`, `withColumn`, `filter`, `dropDuplicates`
// MAGIC - Column: `isNotNull`

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Extract purchase revenue for each event
// MAGIC Add new column **`revenue`** by extracting **`ecommerce.purchase_revenue_in_usd`**

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Filter events where revenue is not null
// MAGIC Filter for records where **`revenue`** is not **`null`**

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Check what types of events have revenue
// MAGIC Find unique **`event_name`** values in **`purchasesDF`** in one of two ways:
// MAGIC - Select "event_name" and get distinct records
// MAGIC - Drop duplicate records based on the "event_name" only
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> There's only one event associated with revenues

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Drop unneeded column
// MAGIC Since there's only one event type, drop **`event_name`** from **`purchasesDF`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Chain all the steps above excluding step 3

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
