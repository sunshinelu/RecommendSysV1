package com.ecloud.Inglory.hbaseOperation

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession


/**
 * Created by sunlu on 17/3/27.
 * 参考链接：http://blog.csdn.net/high2011/article/details/52495048
 * http://www.th7.cn/db/nosql/201609/204393.shtml
 */
object combineHbase {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("combineHbase").getOrCreate()
    val sc = spark.sparkContext

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")//yyyy-MM-dd HH:mm:ss

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "ztb-test-yangch-2_webpage")
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入不是同一个表t_userProfileV1

//如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(args(1))) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(args(1))
      tableDesc.addFamily(new HColumnDescriptor("p".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }


    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))//time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename"))//

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取HBASE数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("h"), Bytes.toBytes("websitename")) //时间列
      (urlID, title, content, label, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if(null != x._2) Bytes.toString(x._2) else ""
        val content_1 = Bytes.toString(x._3)
        val label_1 = if(null != x._4)  Bytes.toString(x._4) else ""
        //时间格式转化
        val time_1 = if(null != x._5){
          val time = Bytes.toLong(x._5)
          val date: Date = new Date(time)
          val temp = dateFormat.format(date)
          temp } else ""

        val websitename_1 = if(null != x._6) Bytes.toString(x._6) else ""
        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.filter(x => x._3.length > 0 )

    hbaseRDD.map { x => {
      val key = Bytes.toBytes(x._1)
      val put = new Put(key)
      put.add(Bytes.toBytes("p"), Bytes.toBytes("t"), Bytes.toBytes(x._2.toString))
      put.add(Bytes.toBytes("p"), Bytes.toBytes("c"), Bytes.toBytes(x._3.toString))
      put.add(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._4.toString))
      put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Bytes.toBytes(x._5.toString))
      put.add(Bytes.toBytes("p"), Bytes.toBytes("websitename"), Bytes.toBytes(x._6.toString))

      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)



  }
}
