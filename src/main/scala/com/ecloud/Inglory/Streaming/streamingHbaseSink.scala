package com.ecloud.Inglory.Streaming

import java.text.SimpleDateFormat

import _root_.kafka.serializer.StringDecoder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by sunlu on 17/4/13.
 * 接收Kafka数据，对数据进行清洗，然后使用spark streaming无状态转化操作将结果保存到HBASE中。
 */

object streamingHbaseSink {

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


    val conf = new SparkConf().setAppName("streamingHbaseSink").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext  = new SQLContext(sc)
    val ssc = new StreamingContext(conf, Seconds(20))

    val topic = "testTopic"
    val topicSet = topic.split(" ").toSet

    //create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicSet
    )
    val lines = messages.map(_._2).map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView(line(4), line(5), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val logsRDD3 = lines.filter(x => x.REQUEST_URI.contains("search/getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        val time = dateFormat.parse(x.CREATE_TIME)
        Schema1(userID, urlString)
      }).filter(_.itemString.length >= 10).map{x => ((x.userString, x.itemString), 1)}.reduceByKey(_ + _).map(x => {
      (x._1._1, x._1._2, x._2)
    })


    val tableName = "tableName"

    logsRDD3.saveAsTextFiles("")
    logsRDD3.foreachRDD(rdd => {
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
       // hbaseConf.set("hbase.master", hbaseMaster)
        val jobConf = new Configuration(hbaseConf)
        jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)
        rdd.map(rec => {
          val key = Bytes.toBytes(rec._1 + rec._2 + rec._3)
          val put = new Put(key)
          put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(rec._1.toString)) //userString
          put.add(Bytes.toBytes("info"), Bytes.toBytes("itemID"), Bytes.toBytes(rec._2.toString)) //itemString
          put.add(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(rec._3.toString)) //value
         // put.add(columnFamilyName.getBytes, columnName.getBytes, Bytes.toBytes(rec._2 / (windowSize / batchInterval)))
          (rec._1, put)
        }).saveAsNewAPIHadoopDataset(jobConf)
      })


    //val logsDS = sqlContext.createDataset(logsRDD3).na.drop(Array("userString"))






    //
  //  lines.print()
    //val words: DStream[String] = lines.flatMap(_.split("\n"))
   // words.count().print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
