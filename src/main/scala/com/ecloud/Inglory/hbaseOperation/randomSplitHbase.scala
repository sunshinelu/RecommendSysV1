package com.ecloud.Inglory.hbaseOperation

import java.util.{Properties, Date}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/10.
 */
object randomSplitHbase {

  case class Schema(val rowkey: String, val title: String, val content: String, val title2: String, val content2: String)

  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("randomSplitHbase").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-total_webpage")

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
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列

      (urlID, title, content)
    }
    }.filter(x => null != x._2 & null != x._3).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val content_2: String = new String(x._3, "UTF-8")
        //只提取标题中的中文
        val reg = """[\u4e00-\u9fa5]+""".r
        val mTitle = reg.findAllMatchIn(title_1)
        val titleString = mTitle.mkString("")
        //只提取内容中的中文
        val mContent = reg.findAllMatchIn(content_1)
        val contentString = mContent.mkString("")

        (urlID_1, title_1, content_2, titleString, contentString)
      }
      }.filter(x => x._4.length > 5 & x._5.length > 50).filter(x => ! x._3.contains(",")).map(x => {
      val rowkey = x._1
      val title = x._2//.replace(",", "，")
      val content = x._3.replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")//.replace("\\", "\\\\")//
      Schema(rowkey, title, content, x._4, x._5)
    })

    val totalNumber = hbaseRDD.count()
    val n = 1000
    val rat = (n.toDouble / totalNumber.toDouble).toDouble

    //val exampleRDD = hbaseRDD.randomSplit(Array(0.17, 0.83))(0)

    import spark.implicits._

    val hbaseDS = spark.createDataset(hbaseRDD)
    val hbaseDSprep = hbaseDS.dropDuplicates("title2").dropDuplicates("rowkey").dropDuplicates("content2").select("rowkey", "title", "content")

    val exampleDS = hbaseDSprep.randomSplit(Array(0.23, 0.77), 1234)(0)

   // val exampleDS2 = exampleDS.dropDuplicates("title2").dropDuplicates("rowkey").dropDuplicates("content2").select("rowkey", "title", "content")
    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    exampleDS.write.mode("overwrite").jdbc(url2, "ylzx_Example", prop2)//overwrite
    hbaseDSprep.write.mode("append").jdbc(url2, "ylzx_Example2", prop2)//overwrite

  }
}
