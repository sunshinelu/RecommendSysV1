package com.ecloud.Inglory.KeyWords

import org.ansj.app.keyword.KeyWordComputer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/3/17.
 */
object getKeyWords {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("getKeyWords").getOrCreate()
    val sc = spark.sparkContext
    //load stopwords file
    val stopwordsPath = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsPath).collect().toList

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yeeso_webpage")
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content
    //scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label

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
     // val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))
      
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
        val filterWords: Array[String] = keywords.filter(x => !stopwords.contains(x))
        val keywords_1 = filterWords.mkString(";")
        (x._1, keywords_1)
      }
      }.flatMap(_._2.split(";")).map(x=>(x, 1)).reduceByKey(_ + _).sortBy(_._2, false).filter(_._2 >= 10)
      //.flatMap(_._2.split(";")).distinct()


    //防止路径下该文件夹存在
    val filepath = "/personal/sunlu/lulu/yeeso/getKeyWords"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true)
    } catch {
      case _: Throwable => {}
    }

    // Save and load model
    hbaseRDD.saveAsTextFile(filepath) //没有overwrite功能


    sc.stop()
  }
}
