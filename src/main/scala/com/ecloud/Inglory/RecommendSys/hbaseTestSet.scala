package com.ecloud.Inglory.RecommendSys

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by sunlu on 17/2/8.
 */
object hbaseTestSet extends Serializable{
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val sparkConf = new SparkConf().setAppName("hbaseTestSet")
    val sc = new SparkContext(sparkConf)
    //load stopwords file
    val hdfs = "hdfs://192.168.37.21:8020/user/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(hdfs).collect.toList


    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "yeeso_webpage") //设置输入表名 第一个参数
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, "t_yeeso100") //设置输出表名，与输入是同一个表
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val jobConf = new JobConf(conf)

    //扫描整个表
    val scan = new Scan()
    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据
    val yeesoDate = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"))
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c"))
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"))
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("lab"))
      val time = v.getValue(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified"))
      (key, title, content, url, label, time)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val url_1 = Bytes.toString(x._4)
        val label_1 = Bytes.toString(x._5)
        val time_1 = Bytes.toString(x._6)
        (x._1, title_1, content_1, url_1, label_1, time_1)
      }
      }.filter(x => x._3.length > 0 & x._4.length > 0  & x._5.length > 0  & x._6.length > 0).randomSplit(Array(0.2,0.8),11L)

    val rdd = yeesoDate(0).map{x => {
      val key = x._1
      val put = new Put(key)
      put.add(Bytes.toBytes("p"), Bytes.toBytes("t"), Bytes.toBytes(x._2)) //标签的family:qualify
      put.add(Bytes.toBytes("p"), Bytes.toBytes("c"), Bytes.toBytes(x._3))
      put.add(Bytes.toBytes("f"), Bytes.toBytes("bas"), Bytes.toBytes(x._4))
      put.add(Bytes.toBytes("p"), Bytes.toBytes("lab"), Bytes.toBytes(x._5))
      put.add(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified"), Bytes.toBytes(x._6))

      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }
}
