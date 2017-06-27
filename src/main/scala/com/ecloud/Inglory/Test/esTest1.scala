package com.ecloud.Inglory.Test

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

/**
 * Created by sunlu on 17/3/9.
 */
object esTest1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ESDemo1")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

   // sc.makeRDD(Seq(numbers,airports)).saveToEs("spark/docs")
  }
}
