// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Matrix Multiplication")
  .getOrCreate()
// Create M Dataframe
val matrixM = spark.createDataFrame(Seq(
  (1,1,-2.0),
  (0,0,5.0),
  (2,2,6.0),
  (0,1,-3.0),
  (3,2,7.0),
  (0,2,-1.0),
  (1,0,3.0),
  (1,2,4.0),
  (2,0,1.0),
  (3,0,-4.0),
  (3,1,2.0),
  (2,2,0.0)
)).toDF("i", "j", "v")

// Create N Dataframe
val matrixN = spark.createDataFrame(Seq(
  (1,0,3.0),
  (0,0,5.0),
  (1,2,-2.0),
  (2,0,9.0),
  (0,1,-3.0),
  (0,2,-1.0),
  (1,1,8.0),
  (2,1,4.0),
  (2,2,0.0)
)).toDF("i", "j", "v")

matrixM.createOrReplaceTempView("matrixM")
matrixN.createOrReplaceTempView("matrixN")

// Spark SQl query
val finalresult = spark.sql("""
  SELECT m1.i, n1.j, SUM(m1.v * n1.v) AS v
  FROM matrixM m1
  JOIN matrixN n1
  ON m1.j = n1.i
  GROUP BY m1.i, n1.j
  ORDER BY m1.i, n1.j
""")

finalresult.show()


// COMMAND ----------


