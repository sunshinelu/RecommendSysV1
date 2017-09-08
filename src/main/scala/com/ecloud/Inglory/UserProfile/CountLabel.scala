package com.ecloud.Inglory.UserProfile

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/9/7.
 *
 * 分析最近一月的用户访问的文章的manuallabel
 *
 * 保存：
 * 主键
 * 用户ID
 * manuallabel串：词：词频；词：词频；（从高到底排序，取top30）
 * 时间：年月日
 *
 * 一天分析一次，分析时添加喜好权限
 *
CREATE TABLE `YLZX_TJ_YHXW` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `YHID` varchar(36) DEFAULT NULL COMMENT '用户ID',
  `WZBQTJ` text COMMENT '网站标签统计前三十',
  `XZQHTJ` text COMMENT '行政区划统计前二十',
  `WZLBTJ` text COMMENT '网站类别统计前十',
  `WZTJ` varchar(255) DEFAULT NULL COMMENT '该用户访问的前三网站',
  `CJSJ` varchar(19) DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 *
 */
object CountLabel {
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class LogView(CREATE_BY_ID: String, CREATE_TIME_L: Long, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: String, value: Double)


  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

    // 获取时间
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
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      (userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val creatTime = Bytes.toString(x._2)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat2.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, creatTimeS, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
    ).filter(_.CREATE_TIME_L >= nDaysAgoL).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        //        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 =
          """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        val time = x.CREATE_TIME
        val value = 1.0
        val rating = x.REQUEST_URI match {
          case r if (r.contains("getContentById.do")) => 1.0 * value //0.2
          case r if (r.contains("like/add.do")) => 1.0 * value //0.3
          case r if (r.contains("favorite/add.do")) => 1.0 * value //0.5
          case r if (r.contains("addFavorite.do")) => 1.0 * value //0.5
          case r if (r.contains("favorite/delete.do")) => -1.0 * value //-0.5
          case r if (r.contains("delFavorite.do")) => -1.0 * value //-0.5
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5)

    hbaseRDD
  }

  case class LabelView(itemString: String, manuallabel: String, webLabel: String, webName: String, dist: String)

  def getLabelsRDD(ylzxTable: String, sc: SparkContext): RDD[LabelView] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名称
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val webLabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名称
      val dist = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划
      (urlID, manuallabel, webLabel, webName, dist)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 = Bytes.toString(x._2)
        val webLabel_1 = Bytes.toString(x._3)
        val webName_1 = Bytes.toString(x._4)
        val dist_1 = Bytes.toString(x._5)
        LabelView(urlID_1, manuallabel_1, webLabel_1, webName_1, dist_1)
      }
      ).filter(_.manuallabel.length >= 2)
    /*.flatMap {
    case (id, manuallabel) => manuallabel.split(";").map((id, _))
  }.map(x => {
    val rowkey = x._1
    val label = x._2
    val value = 1.0
    LabelView(rowkey, label, value)
  })
*/
    hbaseRDD
  }

  case class WZBQTJschema(YHID: String, WZBQTJ: String)

  //网站标签
  case class XZQHTJschema(YHID: String, XZQHTJ: String)

  //行政区划
  case class WZLBTJschema(YHID: String, WZLBTJ: String)

  //网站类别
  case class WZTJschema(YHID: String, WZTJ: String)

  //网站名称


  def main(args: Array[String]) {
    // 不输出日志
    SetLogger

    /*
    1. bulid spark environment
     */

    val sparkConf = new SparkConf().setAppName(s"CountLabel") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val ylzxTable = args(0)
    val logsTable = args(1)

    /*
        val ylzxTable = "yilan-total_webpage"
        val logsTable = "t_hbaseSink"
    */

    // 获取日志数据
    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).groupBy("userString", "itemString").agg(sum("value")).drop("value").
      withColumnRenamed("sum(value)", "value")


    // 获取易览资讯标签数据
    val labelsRDD = getLabelsRDD(ylzxTable, sc)
    val labelsDS = spark.createDataset(labelsRDD)

    /*
    对标签数据进行数据清洗
     */
    // 获取文章标签
    val manuallableDf = labelsDS.select("itemString", "manuallabel").
      withColumn("labels", explode(split($"manuallabel", ";"))).
      withColumn("rating", lit(1.0)).drop("manuallabel").
      filter(length($"labels") >= 2)
    // 获取网站类别
    val webLabelDf = labelsDS.select("itemString", "webLabel").withColumn("rating", lit(1.0)).
      filter(length($"webLabel") >= 2)
    // 获取网站名称
    val webNameDf = labelsDS.select("itemString", "webName").withColumn("rating", lit(1.0)).
      filter(length($"webName") >= 2)
    // 获取行政区划
    val distDf = labelsDS.select("itemString", "dist").withColumn("rating", lit(1.0)).
      filter(length($"dist") >= 2)

    // 分组排序窗口函数
    val w = Window.partitionBy("userString").orderBy(col("score").desc)
    // 合并两列数据
    val combinColUDF = udf((col1: String, col2: Double) => col1 + ":" + col2.toString)


    // 文章标签取前30
    val user_manuallableDf = logsDS.join(manuallableDf, Seq("itemString"), "left").na.drop().
      withColumn("score", round($"value" * $"rating")).drop("value").drop("rating").
      groupBy("userString", "labels").agg(sum("score")).drop("score").withColumnRenamed("sum(score)", "score").
      withColumn("rn", row_number.over(w)).where(col("rn") <= 30).drop("rn").
      withColumn("WZBQTJ", combinColUDF($"labels", $"score")).
      select("userString", "WZBQTJ")

    // 网站类别取前10
    val user_webLabelDf = logsDS.join(webLabelDf, Seq("itemString"), "left").na.drop().
      withColumn("score", round($"value" * $"rating")).drop("value").drop("rating").
      groupBy("userString", "webLabel").agg(sum("score")).drop("score").withColumnRenamed("sum(score)", "score").
      withColumn("rn", row_number.over(w)).where(col("rn") <= 10).drop("rn").
      withColumn("WZLBTJ", combinColUDF($"webLabel", $"score")).
      select("userString", "WZLBTJ")

    // 网站名称取前3
    val user_webNameDf = logsDS.join(webNameDf, Seq("itemString"), "left").na.drop().
      withColumn("score", round($"value" * $"rating")).drop("value").drop("rating").
      groupBy("userString", "webName").agg(sum("score")).drop("score").withColumnRenamed("sum(score)", "score").
      withColumn("rn", row_number.over(w)).where(col("rn") <= 3).drop("rn").
      withColumn("WZTJ", combinColUDF($"webName", $"score")).
      select("userString", "WZTJ")

    // 行政区划取前20
    val user_distDf = logsDS.join(distDf, Seq("itemString"), "left").na.drop().
      withColumn("score", round($"value" * $"rating")).drop("value").drop("rating").
      groupBy("userString", "dist").agg(sum("score")).drop("score").withColumnRenamed("sum(score)", "score").
      withColumn("rn", row_number.over(w)).where(col("rn") <= 20).drop("rn").
      withColumn("XZQHTJ", combinColUDF($"dist", $"score")).
      select("userString", "XZQHTJ")

    /*
    分组合并问题
     */

    // 文章标签
    val user_manuallableRdd = user_manuallableDf.rdd.
      map { case Row(userString: String, words: String) => (userString, words) }.reduceByKey(_ + ";" + _).
      map(x => {
        val userString = x._1.toString
        val solrWords = x._2.toString
        WZBQTJschema(userString, solrWords)
      })
    val df_m = spark.createDataset(user_manuallableRdd)

    // 网站类别
    val user_webLabelRdd = user_webLabelDf.rdd.map { case Row(userString: String, words: String) => (userString, words) }.reduceByKey(_ + ";" + _).
      map(x => {
        val userString = x._1.toString
        val solrWords = x._2.toString
        WZLBTJschema(userString, solrWords)
      })
    val df_l = spark.createDataset(user_webLabelRdd)

    //网站名
    val user_webNameRdd = user_webNameDf.rdd.map { case Row(userString: String, words: String) => (userString, words) }.reduceByKey(_ + ";" + _).
      map(x => {
        val userString = x._1.toString
        val solrWords = x._2.toString
        WZTJschema(userString, solrWords)
      })
    val df_t = spark.createDataset(user_webNameRdd)

    // 行政区划
    val user_distRdd = user_distDf.rdd.map { case Row(userString: String, words: String) => (userString, words) }.reduceByKey(_ + ";" + _).
      map(x => {
        val userString = x._1.toString
        val solrWords = x._2.toString
        XZQHTJschema(userString, solrWords)
      })
    val df_d = spark.createDataset(user_distRdd)

    // 数据合并
    val joinedDf = df_m.join(df_l, Seq("YHID"), "outer").join(df_t, Seq("YHID"), "outer").join(df_d, Seq("YHID"), "outer").
      withColumn("CJSJ", current_timestamp()).withColumn("CJSJ", date_format($"CJSJ", "yyyy-MM-dd"))
    /*
    joinedDf.printSchema
    root
     |-- YHID: string (nullable = true)
     |-- WZBQTJ: string (nullable = true)
     |-- WZLBTJ: string (nullable = true)
     |-- WZTJ: string (nullable = true)
     |-- XZQHTJ: string (nullable = true)
     |-- CJSJ: string (nullable = false)
     */

//    val myID = "175786f8-1e74-4d6c-94e9-366cf1649721"
//    joinedDf.filter($"YHID" === myID).show

    /*
// 将joinedDf保存到YLZX_TJ_YHXW表中，本地测试
val url2 = "jdbc:mysql://localhost:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
//使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
val prop2 = new Properties()
prop2.setProperty("user", "root")
prop2.setProperty("password", "root")
joinedDf.write.mode("append").jdbc(url2, "YLZX_TJ_YHXW", prop2)

*/

    //将joinedDf保存到YLZX_TJ_YHXW表中
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")
    joinedDf.write.mode("append").jdbc(url2, "YLZX_TJ_YHXW", prop2)


    sc.stop()
    spark.stop()
  }
}
