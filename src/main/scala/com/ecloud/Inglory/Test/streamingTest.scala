package com.ecloud.Inglory.Test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.streaming.kafka._
import org.apache.log4j.{Level, Logger}


/**
 * Created by sunlu on 17/4/17.
 * //服务器：slave4
 * spark-shell --jars /root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar
 * spark－shell模式下运行成功！
 */
object streamingTest {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
  def main(args: Array[String]) {

    SetLogger
   // Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    //Logger.getLogger("org.eclipse.jrtty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val zkQuorum = "bigdata1.yiyun:2181"
    val group = "test-ylzx"
    val topics = "lqzTest"
    val numThreads = 1
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group,topicMap)
    val lines = lineMap.map(_._2)
    //lines.print()

    val words = lines.map(_.split(" ")).map(x => (x(0), x(1), x(2)))
    val tableName = "t_hbaseSink"//creat 't_hbaseSink','info'

    //words.print()

    words.foreachRDD(rdd => {
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

    ssc.start
    ssc.awaitTermination
  }
}
