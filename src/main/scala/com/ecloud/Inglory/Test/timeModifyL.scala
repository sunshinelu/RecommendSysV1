package com.ecloud.Inglory.Test

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/19.
 */
object timeModifyL {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {

    // build spark environment
    val spark = SparkSession.builder().appName("timeModifydL").getOrCreate()
    val sc = spark.sparkContext

    val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, ylzxTable) //设置输出表名，与输入是同一个表t_userProfileV1


    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.setMaxVersions(3)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, time)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toLong(x._2).toString
        val time2 = new String(x._2, "UTF-8")
        (rowkey, time)
      }
      }.filter(_._2.length == 13)
/*
      .filter(x => x._2.contains("-") && x._2.contains(":")).map(x => {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val creatTimeD = dateFormat.parse(x._2)
      val creatTimeS = dateFormat.format(creatTimeD)
      val creatTimeL = dateFormat.parse(creatTimeS).getTime

      (x._1, creatTimeL)
    })
*/

    hbaseRDD.map { x => {
      val key = Bytes.toBytes(x._1.toString)
      val put = new Put(key)
      put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Bytes.toBytes(x._2.toLong))

      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(jobConf)





  }
}
