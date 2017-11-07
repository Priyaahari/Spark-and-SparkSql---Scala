package com.myAssign.scala
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
object q4sql
{

  def main(args: Array[String])
  {

    val sc = SparkSession.builder().master("local").appName("q4sql").getOrCreate()
    import sc.implicits._

    val business = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/business.csv").map(line =>line.split("::"))
    var businessFilter = business.filter(line => line(1).matches(".*Palo Alto,+\\s+CA.*")).map(line=>(line(0),line(1),line(2)))

    //creating data frame for business data
    val schemaString = "businessId address category"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    //converting RDD into row
    val businessToRow = businessFilter.map(x => Row(x._1, x._2, x._3))
    //creating data frame
    val t1 = sc.createDataFrame(businessToRow, schema)
    t1.createOrReplaceTempView("t1")
    t1.show()

    //creating data frame for review data
    val review = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/review.csv").map(line =>line.split("::"))
    var reviewMap = review.map(line=>(line(2),line(1),line(3)))
    val schemaString1 = "businessId userId rating"
    val fields1 = schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema1 = StructType(fields1)
    //converting RDD into row
    val reviewToRow = reviewMap.map(x => Row(x._1, x._2, x._3))
    //creating data frame
    val t2 = sc.createDataFrame(reviewToRow, schema1)
    t2.createOrReplaceTempView("t2")
    t2.show()

    //Now join two tables to find userid and rating information
    val t3 = sc.sql("select distinct userId, rating from t1 left join t2 on t2.businessId=t1.businessId")
    t3.createOrReplaceGlobalTempView("t4")
    t3.show()



  }
}
