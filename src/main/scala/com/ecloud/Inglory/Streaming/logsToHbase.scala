package com.ecloud.Inglory.Streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/4/19.
 * 接收Kafka传过来的日志数据，使用spark streaming保存到HBASE中
 * slave4 运行成功！
 * 
spark-submit --class com.ecloud.Inglory.Streaming.logsToHbase \
--master yarn --num-executors 8 \
--executor-cores 8 \
--executor-memory 4g \
--jars /root/lulu/Program/jarLibs/spark-streaming-kafka_2.11-1.6.3.jar,/root/lulu/Program/jarLibs/spark-streaming-flume_2.11-2.1.0.jar,/root/lulu/Program/jarLibs/kafka_2.11-0.10.0.1.jar,/root/lulu/Program/jarLibs/zkclient-0.8.jar,/root/lulu/Program/jarLibs/metrics-core-2.2.0.jar,/root/lulu/Program/jarLibs/metrics-annotation-2.2.0.jar,/root/lulu/Program/jarLibs/kafka-clients-0.10.0.1.jar \
/root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar

 */
object logsToHbase {

  /*
  `LOG_ID` varchar(36) CHARACTER SET utf8 NOT NULL COMMENT '编号',
  `TYPE` char(1) CHARACTER SET utf8 DEFAULT '1' COMMENT '日志类型  1：接入日志；2：错误日志',
  `TITLE` varchar(255) CHARACTER SET utf8 DEFAULT '' COMMENT '日志标题',
  `CREATE_BY` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '创建者',
  `CREATE_BY_ID` varchar(36) CHARACTER SET utf8 DEFAULT NULL COMMENT '登录人ID',
  `CREATE_TIME` varchar(30) CHARACTER SET utf8 DEFAULT NULL COMMENT '创建时间',
  `REMOTE_ADDR` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '操作IP地址',
  `USER_AGENT` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '用户代理',
  `REQUEST_URI` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '请求URI',
  `METHOD` varchar(5) CHARACTER SET utf8 DEFAULT NULL COMMENT '操作方式',
  `PARAMS` text CHARACTER SET utf8 COMMENT '操作提交的数据',
   */
  //LOG_ID, TYPE, TITLE, CREATE_BY, CREATE_BY_ID, CREATE_TIME, REMOTE_ADDR, USER_AGENT, REQUEST_URI, METHOD, PARAMS
  //编号, 日志类型(1：接入日志；2：错误日志), 日志标题, 创建者, 登录人ID, 创建时间, 操作IP地址, 用户代理, 请求URI, 操作方式, 操作提交的数据
  case class LogsSchema(LOG_ID: String, TYPE: String, TITLE: String, CREATE_BY: String, CREATE_BY_ID: String,
                        CREATE_TIME: String,REMOTE_ADDR: String, USER_AGENT: String, REQUEST_URI: String,
                        METHOD: String, PARAMS: String)

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(20))
    //val zkQuorum = "bigdata1.yiyun:2181"
    val zkQuorum = "bigdata1.yiyun:2181,bigdata2.yiyun:2181,bigdata4.yiyun:2181,bigdata5.yiyun:2181,slave3.yiyun:2181"
    val group = "test-ylzx"
    val topics = "ylzx"//ylzx or lqzTest
    val numThreads = 1
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group,topicMap)

    /*
    val lines = messages.map(_._2).map(_.split("\t")).filter(_.length == 11).filter(null != _ (0)).filter(_ (4).length > 2).map(x => {
      val LOG_ID = x(0)
      val TYPE = if(null != x(1)) x(1) else ""
      val TITLE = if(null != x(2)) x(2) else ""
      val CREATE_BY = if(null != x(3)) x(3) else ""
      val CREATE_BY_ID = if(null != x(4)) x(4) else ""
      val CREATE_TIME = if(null != x(5)) x(5) else ""
      val REMOTE_ADDR = if(null != x(6)) x(6) else ""
      val USER_AGENT = if(null != x(7)) x(7) else ""
      val REQUEST_URI = if(null != x(8)) x(8) else ""
      val METHOD = if(null != x(9)) x(9) else ""
      val PARAMS = if(null != x(10)) x(10) else ""
      LogsSchema(LOG_ID, TYPE, TITLE, CREATE_BY, CREATE_BY_ID, CREATE_TIME, REMOTE_ADDR, USER_AGENT, REQUEST_URI, METHOD, PARAMS)
    })
*/

    val lines = messages.map(_._2).map(_.split("\t")).filter(_.length == 11).map(x => {
      val LOG_ID = x(0)
      val TYPE = if(null != x(1)) x(1) else ""
      val TITLE = if(null != x(2)) x(2) else ""
      val CREATE_BY = if(null != x(3)) x(3) else ""
      val CREATE_BY_ID = if(null != x(4)) x(4) else ""
      val CREATE_TIME = if(null != x(5)) x(5) else ""
      val REMOTE_ADDR = if(null != x(6)) x(6) else ""
      val USER_AGENT = if(null != x(7)) x(7) else ""
      val REQUEST_URI = if(null != x(8)) x(8) else ""
      val METHOD = if(null != x(9)) x(9) else ""
      val PARAMS = if(null != x(10)) x(10) else ""
      LogsSchema(LOG_ID, TYPE, TITLE, CREATE_BY, CREATE_BY_ID, CREATE_TIME, REMOTE_ADDR, USER_AGENT, REQUEST_URI, METHOD, PARAMS)
    })
    //lines.print()

    val tableName = "t_hbaseSink"//creat 't_hbaseSink','info'



    lines.foreachRDD(rdd => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      // hbaseConf.set("hbase.master", hbaseMaster)
      val jobConf = new Configuration(hbaseConf)
      jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)
      rdd.map(rec => {
        val key = Bytes.toBytes(rec.LOG_ID)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("logID"), Bytes.toBytes(rec.LOG_ID.toString)) //LOG_ID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("tYPE"), Bytes.toBytes(rec.TYPE.toString)) //TYPE
        put.add(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY"), Bytes.toBytes(rec.CREATE_BY.toString)) //CREATE_BY
        put.add(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID"), Bytes.toBytes(rec.CREATE_BY_ID.toString)) //CREATE_BY_ID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME"), Bytes.toBytes(rec.CREATE_TIME.toString)) //CREATE_TIME
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rEMOTE_ADDR"), Bytes.toBytes(rec.REMOTE_ADDR.toString)) //REMOTE_ADDR
        put.add(Bytes.toBytes("info"), Bytes.toBytes("uSER_AGENT"), Bytes.toBytes(rec.USER_AGENT.toString)) //USER_AGENT
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI"), Bytes.toBytes(rec.REQUEST_URI.toString)) //REQUEST_URI
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mETHOD"), Bytes.toBytes(rec.METHOD.toString)) //METHOD
        put.add(Bytes.toBytes("info"), Bytes.toBytes("pARAMS"), Bytes.toBytes(rec.PARAMS.toString)) //PARAMS

        // put.add(columnFamilyName.getBytes, columnName.getBytes, Bytes.toBytes(rec._2 / (windowSize / batchInterval)))
        (rec.LOG_ID, put)
        //(new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(jobConf)
    })

    ssc.start
    ssc.awaitTermination
  }
}
