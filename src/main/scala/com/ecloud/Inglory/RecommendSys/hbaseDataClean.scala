package com.ecloud.Inglory.RecommendSys


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import org.jsoup.Jsoup

/**
 * Created by sunlu on 17/2/6.
 * 读取ycxpath_webpage中的p:c列，并将结果保存到yeeso-test-ycxpath_webpage_prep表中的info:c列
 *yeeso-test-ycxpath_webpage_prep创建命令:create 'yeeso-test-ycxpath_webpage_prep','info'
 */
object hbaseDataClean {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val sparkConf = new SparkConf().setAppName("hbaseDataClean")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "yeeso-test-ycxpath_webpage") //设置输入表名 第一个参数
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, "yeeso-test-ycxpath_webpage_prep") //设置输出表名
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取表中数据，并对数据进行过滤
    val hbaseData = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c"))
      (key, content)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val content_1 = Bytes.toString(x._2)
        val doc = Jsoup.parse(content_1.toString).body().text()
        (rowkey_1, doc)
      }
      }.map { x => {
      val key = Bytes.toBytes(x._1)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("c"), Bytes.toBytes(x._2.toString)) //标签的family:qualify
      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }

}

