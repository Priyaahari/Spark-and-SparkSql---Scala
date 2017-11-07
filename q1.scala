package com.myAssign.scala
import org.apache.spark._

object q1
{

  def main(args: Array[String])
  {

    val sc = new SparkContext("local", "q1")
    val lines = sc.textFile("/home/priyaa/Documents/Assignment 2/soc-LiveJournal1Adj.txt")

    // parse the data
    val inputData = lines.map(read).filter(line => line._1 != "-1")

    // modify the data from (0 1,2,3..) to ((0,1),1,2,3...)
    val modifyInput = inputData.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) {
      ((x._1, y), x._2)
    } else {
      ((y, x._1), x._2)
    }))

    // Now, reduce by key to find common frnds
    val reducedInput = modifyInput.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)

    // make sure the user is not in the list
    val reducedMap = reducedInput.map(x => (x._1, x._2._2)).filter(l=>l._2!=List())

    // map the data
    val output=reducedMap.map(x => (x._1.toString()+ "\t"+  x._2.size))

    //output.saveAsTextFile("/home/priyaa/Documents/Assignment 2/hum160030_assignment2/q1/q1Out.txt")
    output.foreach(println)



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
