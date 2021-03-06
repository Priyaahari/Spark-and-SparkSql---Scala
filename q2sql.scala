package com.myAssign.scala
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe._

object q2sql{

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

    //sort top 10 count
    val t5 = sc.sql("select distinct user1, user2, Count(list1 ,list2) as count from t4 sort by count desc")
    t5.createOrReplaceTempView("t5")
    t5.show()


    //Now, read the userdata.txt file and map the data
    val userDetails = sc.sparkContext.textFile("/home/priyaa/Documents/Assignment 2/userdata.txt").map(line=>line.split(","))
    val data2 = userDetails.map(x=> (x(0), x(1),x(2), x(3)))


    val schemaString1 = "userId fname lname address"
    val fields1 = schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema1 = StructType(fields1)

    //converting RDD into row
    val data2ToRow = data2.map(x => Row(x._1, x._2, x._3, x._4))

    //creatig data frame
    val t6 = sc.createDataFrame(data2ToRow, schema1)
    t6.createOrReplaceTempView("t6")
    //tabDet.createOrReplaceTempView("table2")
    //t6.createOrReplaceGlobalTempView("t7")
    t6.show()
    t6.write.saveAsTable("t7")

    val t8 = sc.sql("select t5.user1, t5.user2, t5.count, t6.fname, t6.lname, t6.address, t7.fname , t7.lname , t7.address from t5, t6, t7 where t5.user1 = t6.userId and t5.user2=t7.userId")


   /* val t8 = sc.sql("select t5.count, t5.user1, a.userId, a.fname, a.lname, a.address, b.userId, b.name " +
      "from t5 " +
      "left join t6 a on a.userId = t5.user1 " +
      "left join t6 b on b.userId = t5.user2 ")
      */
    t8.createOrReplaceGlobalTempView("t8")
    t8.show()



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
