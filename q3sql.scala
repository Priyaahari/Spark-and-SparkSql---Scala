package com.myAssign.scala
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf

object q3sql {
  def main(args: Array[String]) {


    val sc = SparkSession.builder().master("local").appName("q3sql").getOrCreate()
    import sc.implicits._

    //val sc = new SparkContext("local[*]", "Q1")
    val review = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/review.csv").map(line => line.split("::"))
    var reviewMap = review.map(line => (line(2), line(1), line(3))).distinct

    val business = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/business.csv").map(line => line.split("::"))
    var businessMap = business.map(line => (line(0), line(1) + " ," + line(2))).distinct


    val schemaString = "businessId userId rating"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //converting RDD into row
    val reviewToRow = reviewMap.map(x => Row(x._1, x._2,x._3))

    //creatig data frame
    val t1 = sc.createDataFrame(reviewToRow, schema)
    t1.createOrReplaceTempView("t1")
    //reviewTable.show()

    val schemaString1 = "businessId address"
    val fields1 = schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema1 = StructType(fields1)

    //converting RDD into row
    val businessToRow = businessMap.map(x => Row(x._1, x._2))

    //creatig data frame
    val t3 = sc.createDataFrame(businessToRow, schema1)
    t3.createOrReplaceTempView("t3")
    t3.show()

    //review data

    val t2 = sc.sql("select businessId, count(userId) as countOfUsers from t1 group by(businessId) sort by countOfUsers desc")
    t2.createOrReplaceGlobalTempView("t2")
    t2.show()
    t2.write.saveAsTable("t22")


    //top 10 review data join with busness data

    val t4 = sc.sql("select * from t22 left join t3 on t22.businessId = t3.businessId")
    t4.createOrReplaceTempView("t4")
    t4.limit(30).show()

  }
}