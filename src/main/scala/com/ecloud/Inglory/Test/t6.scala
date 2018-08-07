package com.ecloud.Inglory.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/16.
 * 获取dataframe中的第一条数据
 */
object t6 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("t6").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = Map("a" -> 1, "b" -> 2, "c" -> 3).toArray
    val df2 = sc.parallelize(df1).toDF("x1","x2").withColumn("x2", $"x2" +1 ).filter($"x2" > 2)
    val t = df2.sort($"x2".desc).first().get(0)
    println(t)

    val seq0 = Map("a" -> 1,
      "b" -> 2,
      "c" -> 3)
    val seq1 =for((k,v) <- seq0) yield (k,v+1)
    val seq2 =seq1.filter( s => s._2 > 2)
    val seq3 = if(!seq2.isEmpty) seq2.maxBy(s =>s._2)._1
    print(seq3)
  }
}
