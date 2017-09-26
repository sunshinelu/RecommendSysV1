package com.ecloud.Inglory.DocsSimilarity.DcoSimiTest

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/9/26.
 */
object CheckTotalTable {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


  case class YlzxSegSchema(urlID: String, title: String, content: String, label: String, time: String, websitename: String,
                           segWords: Seq[String])


  case class YlzxSegSchema2(id: Long, urlID: String, title: String, label: String, time: String, websitename: String,
                            content:String, segWords: Seq[String])

  def getYlzxSegYRDD(ylzxTable: String, year: Int, sc: SparkContext): RDD[YlzxSegSchema2] = {

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = year
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("appc"))

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
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名列
      //val appc = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("appc")) //appc
      (urlID, title, content, label, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3).replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
        val label_1 = Bytes.toString(x._4)
        //时间格式转化
        val time_1 = Bytes.toLong(x._5)
        val websitename_1 = Bytes.toString(x._6)

        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.//filter(x => x._2.length > 1 & x._3.length > 50).//filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time = dateFormat.format(date)
        val content = x._3.toString
        //使用ansj分词
        val segWords = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq
        YlzxSegSchema(x._1, x._2, content, x._4, time, x._6, segWords)
      }).zipWithUniqueId().map(x => {
      val id = x._2
      val urlID = x._1.urlID
      val title = x._1.title
      val content = x._1.content
      val label = x._1.label
      val time = x._1.time
      val websitename = x._1.websitename
      val segWords = x._1.segWords
      YlzxSegSchema2(id, urlID, title, label, time, websitename, content,segWords)
    }).filter(x => null != x.segWords).filter(_.segWords.size > 1) //.randomSplit(Array(0.1,0.9))(0)

    hbaseRDD

  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"CheckTotalTable").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total-analysis_webpage"

    val ylzxRDD = getYlzxSegYRDD(ylzxTable, 1, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).drop("segWords")
    println("ylzxDS is: " + ylzxDS.count())



    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 1
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("appc"))

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
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名列
      //val appc = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("appc")) //appc
      (urlID, title, content, label, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3).replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
        val label_1 = Bytes.toString(x._4)
        //时间格式转化
        val time_1 = Bytes.toLong(x._5)
        val websitename_1 = Bytes.toString(x._6)

        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.//filter(x => x._2.length > 1 & x._3.length > 50).//filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time = dateFormat.format(date)
        val content = x._3.toString
        //使用ansj分词
        val segWords = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq
        YlzxSegSchema(x._1, x._2, content, x._4, time, x._6, segWords)
      }).zipWithUniqueId().map(x => {
      val id = x._2
      val urlID = x._1.urlID
      val title = x._1.title
      val content = x._1.content
      val label = x._1.label
      val time = x._1.time
      val websitename = x._1.websitename
      val segWords = x._1.segWords
      YlzxSegSchema2(id, urlID, title, label, time, websitename, content,segWords)
    }).filter(x => null != x.segWords).filter(_.segWords.size > 1) //.randomSplit(Array(0.1,0.9))(0)





    sc.stop()
    spark.stop()

  }
}
