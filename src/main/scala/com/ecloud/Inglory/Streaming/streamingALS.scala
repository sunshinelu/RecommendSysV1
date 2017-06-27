package com.ecloud.Inglory.Streaming

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/18.
 */
object streamingALS {

  case class LogView(CREATE_BY_ID: String, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)
  case class Schema1(userString: String, itemString: String)

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//

    val spark = SparkSession.builder.appName("streamingALS: Build ALS model in Streaming").getOrCreate()
/*
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  */
  }
}
