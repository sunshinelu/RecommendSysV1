package com.ecloud.Inglory.KeyWords

import org.ansj.app.keyword.KeyWordComputer
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result, HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/13.
 */
object keywordsBeLables extends java.io.Serializable{

  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("keywordsBeLables").getOrCreate()
    val sc = spark.sparkContext
    //load stopwords file
    val stopwordsPath = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsPath).collect().toList

    val keywordsLable = sc.textFile("").collect().toList

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso_webpage

    //如果outputTable表存在，则删除表；如果不存在则新建表。
    val outputTable = args(1)
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(outputTable)
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列

      (rowkey, title, content)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        (rowkey_1, title_1, content_1)
      }
      }.filter(x => x._2.length > 0 & x._3.length > 0).
      map { x => {
        val kwc = new KeyWordComputer(15)
        val result = kwc.computeArticleTfidf(x._2, x._3).toArray
        val keywords = for (ele <- result) yield ele.toString.split("/")(0)
        //remove stopwords
        val filterWords: Array[String] = keywords.filter(x => !stopwords.contains(x)).filter(x => keywordsLable.contains(x))
        val keywords_1 = filterWords.mkString(";")
        (x._1, keywords_1)
      }
      }.filter(null != _._2).filter(_._2.length >= 2)

    hbaseRDD.map(x => {
      val key = Bytes.toBytes(x._1.toString)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("keyWords"), Bytes.toBytes(x._2.toString)) //keywords
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)




  }

}
