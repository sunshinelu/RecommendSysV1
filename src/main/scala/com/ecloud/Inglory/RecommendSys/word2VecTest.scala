package com.ecloud.Inglory.MavenSparkTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/13.
 * 测试成功！
 */
object word2VecTest extends java.io.Serializable {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("Word2VecTest1").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    //val modelPath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Word2VecModel"
    val modelPath = "file:///Users/sunlu/Desktop/Word2VecModel"
    val word2vecModel = Word2VecModel.load(sc, modelPath)




    val keyWords = Vector("科技", "人才")
    val synonyms2 = sc.parallelize(keyWords).map { x =>
      (x, try {
        word2vecModel.findSynonyms(x, 100).map(_._1).mkString(";")
      } catch {
        case e: Exception => ""
      })
    }

    synonyms2.collect().foreach(println)

  }
}
