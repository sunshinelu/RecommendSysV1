package com.ecloud.Inglory.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/2/28.
 */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd=sc.textFile(args(0))
    val wordcount=rdd.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    val wordsort=wordcount.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    println(wordsort)
//    wordsort.saveAsTextFile(args(1))
    sc.stop()

  }
}
