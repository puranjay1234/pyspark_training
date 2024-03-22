Puranjay kwatra
pkwatra@teksystems.com
emp_id = 6282

# PySpark Notes

## Introduction
PySpark is the Python API for Apache Spark, a powerful open-source distributed computing system. It provides an easy-to-use interface for working with large-scale data processing and analytics. This document serves as a guide for getting started with PySpark.

## Apache Spark Architecture
Apache Spark is designed for speed, ease of use, and versatility in various data processing tasks. Its architecture consists of the following key components:

### Spark Core
Spark Core is the foundation of the entire Spark ecosystem. It provides distributed task dispatching, scheduling, and basic I/O functionalities. The core abstraction of Spark is the resilient distributed dataset (RDD), which represents a collection of immutable distributed objects that can be operated on in parallel.

### Spark SQL
Spark SQL provides a DataFrame API and SQL interface for working with structured and semi-structured data. It allows users to query data using SQL syntax as well as manipulate data using DataFrame transformations and actions.

### Spark Streaming
Spark Streaming enables real-time processing of streaming data. It ingests data in mini-batches and processes them using the same RDD abstraction as batch processing.


### GraphX
GraphX is a graph processing library built on top of Spark, providing APIs for manipulating graph-structured data and performing graph algorithms.

### SparkR
SparkR allows users to interact with Spark using the R programming language. It provides R bindings to Spark's APIs, enabling R users to leverage Spark's distributed computing capabilities.


## Getting Started
### Creating a SparkSession
To start using PySpark, you first need to create a SparkSession, which is the entry point to PySpark functionality.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()
```

### Loading Data
PySpark supports loading data from various sources such as CSV, JSON, Parquet, etc.

```python
# Load data from CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Display the schema of the DataFrame
df.printSchema()

# Show the first few rows of the DataFrame
df.show()
```

### Data Exploration and Manipulation
PySpark provides powerful tools for data exploration and manipulation.

```python
# Selecting columns
df.select("column1", "column2").show()

# Filtering data
df.filter(df["column1"] > 10).show()

# Aggregating data
df.groupBy("column1").agg({"column2": "sum"}).show()
```

### Machine Learning with PySpark
PySpark includes a rich set of machine learning algorithms for building and deploying scalable ML pipelines.

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Prepare data for modeling
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df = assembler.transform(df)

# Split data into training and testing sets
train_data, test_data = df.randomSplit([0.7, 0.3])

# Train a linear regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_data)

# Make predictions
predictions = model.transform(test_data)
```

### Writing Data
You can save the processed data or model output to various formats using PySpark.

```python
# Write DataFrame to Parquet
df.write.parquet("path/to/output")

# Save model
model.save("path/to/model")
```

