package com.ecloud.Inglory.RecommendSys

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/2/28.
 */
object Word2VecLoadModel extends java.io.Serializable {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    /*
    val spark = SparkSession.builder.appName("Word2VecTest1").getOrCreate()
    val sc = spark.sparkContext
*/
    val conf = new SparkConf().setAppName("Word2VecTest1").setMaster("local[2]")
    val sc = new SparkContext(conf)


    // load word2vec model
//    val modelPath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Word2VecModel"
    val modelPath = "file:///Users/sunlu/Desktop/Word2VecModel"
    val word2vecModel = Word2VecModel.load(sc, modelPath)
    // find synonyms
    val keyword = "科技"

    val temp2 = SynonymsWords("投标")
    println(temp2)
    sc.stop()
  }

  def SynonymsWords(keyword: String): String = {
    //bulid environment
    val spark = SparkSession.builder.appName("Word2VecTest1").getOrCreate()
    val sc = spark.sparkContext
    // load word2vec model
//    val modelPath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Word2VecModel"
    val modelPath = "file:///Users/sunlu/Desktop/Word2VecModel"
    val word2vecModel = Word2VecModel.load(sc, modelPath)
    // find synonyms
    val synonyms = word2vecModel.findSynonyms(keyword, 10)
    val synonymsString = synonyms.map(_._1).filter(_.length >= 2).mkString(";")
    return synonymsString
  }


}
