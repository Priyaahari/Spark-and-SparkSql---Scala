package com.myAssign.scala
import org.apache.spark._

object q2
{

  def main(args: Array[String])
  {
    val sc = new SparkContext("local", "q2")
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
    val output=reducedMap.map(x => (x._1,  x._2.size))
    //output.foreach(println)

    //sort the user based on number of common frnds
    val sort = sc.parallelize(output.sortBy(_._2).collect().reverse)
    //map the ouput as data1
    val data1 = sort.map(_.swap).map(line => (line._1.toInt, line._2._1.toInt , line._2._2.toInt))

    //outputTabSpace.saveAsTextFile(args(1))
    //outputTabSpace.foreach { println }

    //data1.foreach(println)


    //Now, read the userdata.txt file and map the data
    val userDetails = sc.textFile("/home/priyaa/Documents/Assignment 2/userdata.txt").map(line=>line.split(","))
    val data2 = userDetails.map(x=> (x(0).toInt, (x(1)+" "+x(2)+", "+x(3))))

    //join the data1 with data2 to print all the details
    val data2map = data2.collectAsMap()
    val join = data1.map({case(a,b,c) => ("count: " + a +" ("+b+","+c+") " +  "   user1: " + b +"  details :"+  data2map.get(b).get+ "  user2: " + c +"   details : "+ data2map.get(c).get)})

    //print details users having top 10 common mutual frnds count
    join.take(20).foreach(println)



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
