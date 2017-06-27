package com.ecloud.Inglory.hbaseOperation

import com.ecloud.Inglory.Streaming.logsToHbase.LogsSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/27.
 * 将4月22日之前的日志数据保存到 t_hbaseSink 表中
 */
object localLogsToHbase {


  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("localLogsToHbase").getOrCreate()
    val sc = spark.sparkContext

    val logsRDD = sc.textFile("file:///root/lulu/Workspace/spark/yeeso/RecommendSys/logs/app-ylzx-logs/*.log").
      filter(null != _).map(_.split("\t")).
      filter(_.length == 11).map(x => {
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


    val conf = HBaseConfiguration.create()

    //指定输出格式和输出表名
    val outputTable = "t_hbaseSink"
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    logsRDD.map(rec => {
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
//      (rec.LOG_ID, put)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(jobConf)

  }
}
