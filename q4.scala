package com.myAssign.scala
import org.apache.spark._

object q4
{

  def main(args: Array[String])
  {

    val sc = new SparkContext("local", "q4")
    val business = sc.textFile("/home/priyaa/Documents/Assignment 2/business.csv").map(line =>line.split("::"))
    var businessFilter = business.filter(line => line(1).matches(".*Palo Alto,+\\s+CA.*")).map(line=>(line(0),line(1)+line(2)))

    val review = sc.textFile("/home/priyaa/Documents/Assignment 2/review.csv").map(line =>line.split("::"))
    var reviewMap = review.map(line=>(line(2),line(1)+","+line(3)))

    val joinedMaps = businessFilter.join(reviewMap).distinct.collect
    joinedMaps.sortBy(_._1).foreach{data =>
      val details = data._2
      println(details._2)
    }


  }
}
