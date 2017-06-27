package com.ecloud.Inglory.Test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by pc on 2017-02-22.
 */
object ClusterTest {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val sparkConf = new SparkConf().setAppName("ClusterTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    /*
    conf.set("fs.defaultFS", "hdfs://192.168.37.21:8020")
    conf.set("mapreduce.framework.name", "yarn")
    conf.set("yarn.resourcemanager.cluster-id", "yarn-cluster")
    conf.set("yarn.resourcemanager.hostname", "192.168.37.22", "192.168.37.25")
    conf.set("yarn.resourcemanager.admin.address", "192.168.37.25:8141")
    conf.set("yarn.resourcemanager.address", "bigdata2.yiyun:8050")
    conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.37.25:8025")
    conf.set("yarn.resourcemanager.scheduler.address", "192.168.37.25:8030")

    conf.set("hbase.master", "192.168.37.21:60000")
    conf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.nameserver.address", "192.168.37.21,192.168.37.22")
    conf.set("hbase.regionserver.dns.nameserver",
      "192.168.37.22,192.168.37.23,192.168.37.24")
      */

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "t_yeeso100")
    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"))

    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取表中数据，并对数据进行过滤
    val hbaseData = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val tittle = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"))
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("f"))
      (key, tittle, url)
    }
    }.filter(x => null != x._2 & null != x._3).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val content_1 = Bytes.toString(x._2)
        val url_1 = Bytes.toString(x._2)
        (rowkey_1, content_1, url_1)
      }
      }
    /*
          val filepath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/hbase_t1"
          val hadoopConf = new org.apache.hadoop.conf.Configuration()
          val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
          try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }
          hBaseRDD.saveAsTextFile(filepath)
    */
    val filepath = "/personal/sunlu/lulu/hbase_t1"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true)
    } catch {
      case _: Throwable => {}
    }
    hBaseRDD.saveAsTextFile(filepath)
    sc.stop()

  }
}
