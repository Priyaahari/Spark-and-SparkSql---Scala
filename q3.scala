package com.myAssign.scala

import org.apache.spark._

object q3 {
  def main(args: Array[String]) {


    val sc = new SparkContext("local", "q3")

    //read business data
    val review = sc.textFile("/home/priyaa/Documents/Assignment 2/review.csv").map(line => line.split("::"))
    var reviewMap = review.map(line => (line(2), line(1))).distinct
    val count = reviewMap.groupByKey().map(data => {
      val c = data._2.size;
      (c, data._1)
    })

    //sort the business data -top 10 buisnesses as per number of users rated the business
    val result = count.sortBy(_._2).sortByKey(false, 1).take(10)
    val res = result.map(_.swap)

    //read business data
    val business = sc.textFile("/home/priyaa/Documents/Assignment 2/business.csv").map(line => line.split("::"))
    var businessMap = business.map(line => (line(0), line(1) + " ," + line(2))).distinct

    val parallelTopTen = sc.parallelize(res)

    //print all details for top 10 businesses
    val joinedMaps = businessMap.join(parallelTopTen).collect
    joinedMaps.sortBy(_._1).foreach { data =>
      val businessId = data._1
      val details = data._2
      println(businessId, details._1, details._2)
    }
  }
}