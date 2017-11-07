package com.myAssign.scala
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe._

import scala.reflect.macros.whitebox

object q1sql{

  def main(args: Array[String])  {

    //val sc =  new SparkContext("local[*]", "Q1")
    val sc = SparkSession.builder().master("local").appName("q1sql").getOrCreate()
    import sc.implicits._

    val lines = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/soc-LiveJournal1Adj.txt")

    // parse the data
    val inputData = lines.map(read).filter(line => line._1 != "-1")

    // modify the data from (0 1,2,3..) to ((0,1),1,2,3...)
    val modifyInput = inputData.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) {
      (x._1, y, x._2._2)
    } else {
      (y, x._1, x._2._2)
    }))



    val schemaString = "user1 user2 list"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //converting RDD into row
    val rowRDD = modifyInput.map(x => Row(x._1, x._2, x._3.toString()))

    //creatig data frame
    val table = sc.createDataFrame(rowRDD, schema)
    table.createOrReplaceTempView("t1")

    table.createOrReplaceTempView("t2")

    val t3 = sc.sql("select t1.user1, t1.user2, t1.list as list1, t2.list as list2 from t1, t2 where t1.user1 = t2.user1 and t1.user2 = t2.user2 ")
    t3.createOrReplaceTempView("t3")
    t3.show()

    val t4 = sc.sql("select distinct (user1), user2, list1, list2 from t3 where list1 != list2" )
    t4.createOrReplaceTempView("t4")
    t4.show()


    //val count = udf((l1: List[String], l2: List[String]) => l1.intersect(l2).size)

    def findCount(l1: String, l2: String) = {l1.intersect(l2).size}

    sc.udf.register("Count" , findCount _)

    val t5 = sc.sql("select distinct user1, user2, Count(list1 ,list2) as count from t4 ")
    t5.createOrReplaceTempView("t5")
    t5.show()





    /*

    val reduceRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val reduce_rddPair = reduceRdd.map(r => (r._1, r._2._2)).filter(l=>l._2!=List())
    val output=reduce_rddPair.map(r => (r._1, r._2.size))
    val outputTabSpace = output.map(tup => tup._1.toString() + "\t" + tup._2.toString())


    //outputTabSpace.saveAsTextFile(args(1))
    outputTabSpace.foreach { println }
    */
  }


  def read(input: String): (String, (Long, List[String])) = {
    val split = input.split("\t")
    if (split.length == 2)
    {
      val user = split(0)
      val friendsList = split(1).split(",").toList
      return (user, (1L, friendsList))
    }
    else
    {
      return ("-1", (-1L, List()))
    }
  }
}
