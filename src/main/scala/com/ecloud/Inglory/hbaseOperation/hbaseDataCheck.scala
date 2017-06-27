package com.ecloud.Inglory.hbaseOperation

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/5/20.
 */
object hbaseDataCheck {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("word2Vec: docSimilarity").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "t_userProfileV1") //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //扫描整个表
    val scan = new Scan()

    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //标题列
      val simsID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //内容列
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("t")) //标签列
      val level = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //标签列
      val simsScore = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //标签列
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //时间列
      val time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //网站名列
      val webName = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //appc
      (rowkey, id, simsID, title, level, simsScore, manuallabel, time, webName)
    }
    }.map { x => {
        val rowkey = Bytes.toString(x._1)
        val id = Bytes.toString(x._2)
        val simsID = Bytes.toString(x._3)
        val title = Bytes.toString(x._4)
      val level = Bytes.toString(x._5)
      val simsScore = Bytes.toString(x._6)
      val manuallabel = Bytes.toString(x._7)
      val time = Bytes.toString(x._8)
      val webName = Bytes.toString(x._9)

        (rowkey, id, simsID, title, level, simsScore, manuallabel, time, webName)
      }
      }




  }
}
