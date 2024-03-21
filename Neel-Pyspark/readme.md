# PySpark Notes

#### What is Spark ?
- Apache Spark is an open source distributed computing system designed for big data processing and analytics. Spark is known for its speed and efficiency, thanks to its in-memory computing capabilities and optimized data processing techniques.

#### What is PySpark ?
- Pyspark is a python API to support Python with Apache Spark. PySpark provides Py4j (basically Py4j enables python programs running in a python interpreter to dynamically access Java objects in a java Virtual Machine)
- with Pyspark we can write Python and SQL like commands to manipulate and analyze data.

#### Features of Apache Spark :

- In-memory processing (speed):
    -  Spark leverages in-memory computing to store and process data in memory, resulting in significantly faster data processing compared to disk-based systems like Hadoop MapReduce.

- Distributed Computing:
    -  Spark distributes data and computation across multiple nodes in a cluster, enabling parallel processing and efficient resource utilization.

- Broad Language Support:
    -  Spark support Scala, Java, Python and R

-  Integration with Big Data Ecosystems
    - Spark work with various data storage systems and technologies, including Hadoop Distributed File System (HDFS), HBase, Cassadra and Amazon S3
 
#### Key Concept In Spark:
- For data storage spark use RDD
    - RDD are fundamental data structure in Spark. They represent distributed collections of objects that can be processed in parallel. RDDs are fault-tolerant

- For data processing spark use DAG



