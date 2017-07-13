package com.ecloud.Inglory.appRecom

import java.text.SimpleDateFormat
import java.util.{Properties, Calendar, Date}

import com.ecloud.Inglory.RatingSys.UtilTool
import com.ecloud.Inglory.util.{deleteMysqlData, truncateMysql}
import org.ansj.app.keyword.KeyWordComputer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/6/22.
 * 均一化参考链接：https://stackoverflow.com/questions/33924842/minmax-normalization-in-scala
 * na.fill参考链接：http://www.cnblogs.com/wwxbi/p/6011422.html
 * com.ecloud.Inglory.appRecom.appRecomV1 步骤：
1、读取用户日志数据t_hbaseSink：获取用户浏览、点赞、收藏，其中浏览权重为0.2，点赞权重为0.3，收藏权重为0.5，取消收藏权重为－0.5。
                  时间衰减因子权重：3天以内：0.9
                                  3～7天：0.8
                                  7天～半月：0.7
                                  半月～一月：0.6
                                  1～6月：0.5
                                  6月～1年：0.4
                                  大于一年：0.3
2、读取用户日志数据t_hbaseSink：获取用户搜索的关键词，衰减因子处理方案如上。
3、读取yilan-total_webpage表：获取文章的标签，每篇文章中的标签权重均为1。
4、读取yilan-total_webpage表：对每篇文章进行关键词提取，每篇文章提取的关键词个数为10，每个关键词的权重均为一。
5、对搜索词进行标准化处理：对第2步的搜索词进行分组、加和、标准化操作。
6、对标签词进行标准化处理：将第1步获取的用户数据与第3步的标签数据进行left join。然后分组、加和、标准化处理。
7、对关键词进行标准化处理：将第1步获取的用户数据与第4步的关键词数据进行left join。然后分组、加和、标准化处理。
8、数据合并，将第5、6、7步结果进行full join。
9、权重加和：搜索词权重0.5，标签词权重0.3，关键词权重0.2。
10、数据整形：分组、排序、top40、reduceByKey
11、结果保存到mysql数据库。
 *
 */
object appRecomV1 {

  case class LogView(CREATE_BY_ID: String, CREATE_TIME: Long, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: Long, value: Double)

  case class LogView3(userString: String, rowkey: String, value: Double)

  case class SearchWordsView(userString: String, searchWords: String, CREATE_TIME: Long, value: Double)

  case class SearchWordsView2(userString: String, searchWords: String, searchValue: Double)

  case class LabelView(rowkey: String, label: String, labelValue: Double)

  case class KeyWordsView(rowkey: String, keyWord: String, keyWordsValue: Double)

  case class solrWordsView(OPERATOR_ID: String, INTEREST_WORD: String, CREATE_DATE: String)


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView3] = {

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
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("search/getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do")
    ).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
//        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 = """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        val time = x.CREATE_TIME
        val value = 1.0
        val rating = x.REQUEST_URI match {
          case r if (r.contains("search/getContentById.do")) => 0.2 * value
          case r if (r.contains("like/add.do")) => 0.3 * value
          case r if (r.contains("favorite/add.do")) => 0.5 * value
          case r if (r.contains("favorite/delete.do")) => -0.5 * value
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5).map(x => {
      val userString = x.userString
      val itemString = x.itemString
      val time = x.CREATE_TIME
      val value = x.value

      val rating = time match {
        case x if (x >= UtilTool.get3Dasys()) => 0.9 * value
        case x if (x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys()) => 0.8 * value
        case x if (x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys()) => 0.7 * value
        case x if (x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth()) => 0.6 * value
        case x if (x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth()) => 0.5 * value
        case x if (x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth()) => 0.4 * value
        case x if (x < UtilTool.getOneYear()) => 0.3 * value
        case _ => 0.0
      }

      LogView3(userString, itemString, rating)
    })

    hbaseRDD
  }


  def getSearchWordsRDD(logsTable: String, sc: SparkContext): RDD[SearchWordsView2] = {
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
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, requestURL, parmas)
      }
      }.filter(x => {
      x.REQUEST_URI.contains("searchByKeyword.do") && x.PARAMS.contains("keyword=") && x.CREATE_BY_ID.length >= 5
    }).map(x => {
      val userID = x.CREATE_BY_ID.toString
      val reg = """keyword=.+(,|})""".r
      val searchWord = reg.findFirstIn(x.PARAMS.toString).toString.replace("Some(keyword=", "").replace(",)", "").replace("}", "").replace(")", "")
      val time = x.CREATE_TIME
      val value = 1.0
      SearchWordsView(userID, searchWord, time, value)
    }).filter(x => (x.searchWords.length >= 2 && x.searchWords.length <= 10)).map(x => {
      val userString = x.userString
      val searchWords = x.searchWords
      val time = x.CREATE_TIME
      val value = x.value

      val rating = time match {
        case x if (x >= UtilTool.get3Dasys()) => 0.9 * value
        case x if (x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys()) => 0.8 * value
        case x if (x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys()) => 0.7 * value
        case x if (x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth()) => 0.6 * value
        case x if (x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth()) => 0.5 * value
        case x if (x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth()) => 0.4 * value
        case x if (x < UtilTool.getOneYear()) => 0.3 * value
        case _ => 0.0
      }

      SearchWordsView2(userString, searchWords, rating)
    })

    hbaseRDD
  }


  def getLabelRDD(ylzxTable: String, sc: SparkContext): RDD[LabelView] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      (urlID, manuallabel)
    }
    }.filter(x => null != x._2).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 = Bytes.toString(x._2)
        (urlID_1, manuallabel_1)
      }
      ).filter(_._2.length >= 2).flatMap {
      case (id, manuallabel) => manuallabel.split(";").map((id, _))
    }.map(x => {
      val rowkey = x._1
      val label = x._2
      val value = 1.0
      LabelView(rowkey, label, value)
    })

    hbaseRDD
  }

  def getKeyWordsRDD(ylzxTable: String, sc: SparkContext, stopwords: List[String]): RDD[KeyWordsView] = {
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
    val N = 20
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content列
      (urlID, title, time, content)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        val title = Bytes.toString(x._2)
        //时间格式转化
        val time = Bytes.toLong(x._3)
        val content = Bytes.toString(x._4)
        (rowkey, title, time, content)
      }
      }.filter(x => {
      x._2.length >= 2 && x._2.length >= 20 && x._3 <= todayL & x._3 >= nDaysAgoL
    }).map(x => {
      val rowkey = x._1.toString
      val title = x._2.toString
      val content = x._3.toString
      //每篇文章提取10个关键词
      val kwc = new KeyWordComputer(10)
      val keywords = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).mkString(";")
      (rowkey, keywords)
    }).flatMap { case (rowkey, keywords) => keywords.split(";").map((rowkey, _)) }.map(x => {
      val rowkey = x._1
      val keyword = x._2
      val value = 1.0
      KeyWordsView(rowkey, keyword, value)
    })

    hbaseRDD

  }

  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("appRecom: appRecomV1").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    val ylzxTable = args(0) //yilan-total_webpage
    val logsTable = args(1) //t_hbaseSink

    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD)

    /*
    对搜索词进行标准化
     */
    val searchRDD = getSearchWordsRDD(logsTable, sc)
    val searchDS = spark.createDataset(searchRDD).
      groupBy("userString", "searchWords").agg(sum("searchValue")).
      withColumnRenamed("sum(searchValue)", "searchRating").drop("searchValue")

    val (sMin, sMax) = searchDS.agg(min($"searchRating"), max($"searchRating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val scaledRange = lit(1) // Range of the scaled variable
    val scaledMin = lit(0) // Min value of the scaled variable
    val s_Normalized = ($"searchRating" - sMin) / (sMax - sMin) // v normalized to (0, 1) range
    val search_Scaled = scaledRange * s_Normalized + scaledMin
    val searchDS2 = searchDS.withColumn("searchRating", bround(search_Scaled, 4)).withColumnRenamed("searchWords", "words")
    //userString,searchWords,searchRating

    /*
    对标签词进行标准化
     */
    val labelRDD = getLabelRDD(ylzxTable, sc)
    val labelDS = spark.createDataset(labelRDD)
    val labelDS2 = logsDS.join(labelDS, Seq("rowkey"), "left").na.drop().
      withColumn("labelRating", $"value" * $"labelValue").drop("value").drop("labelValue").drop("rowkey").
      groupBy("userString", "label").agg(sum("labelRating")).drop("labelRating").
      withColumnRenamed("sum(labelRating)", "labelRating")
    //(userString: String, rowkey: String, value: Double, label: String, labelValue: Double)
    //(userString: String,label: String, labelRating: Double)
    //userString, label, labelRating
    val (lMin, lMax) = labelDS2.agg(min($"labelRating"), max($"labelRating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val l_Normalized = ($"labelRating" - lMin) / (lMax - lMin) // v normalized to (0, 1) range
    val label_Scaled = scaledRange * l_Normalized + scaledMin
    val labelDS3 = labelDS2.withColumn("labelRating", bround(label_Scaled, 4)).withColumnRenamed("label", "words")
    //userString, label, labelRating


    /*
    对关键词进行标准化
     */
    val keywordRDD = getKeyWordsRDD(ylzxTable, sc, stopwords)
    val keywordDS = spark.createDataset(keywordRDD)
    val keywordDS2 = logsDS.join(keywordDS, Seq("rowkey"), "left").na.drop().
      withColumn("keyWordsRating", $"value" * $"keyWordsValue").drop("value").drop("keyWordsValue").drop("rowkey").
      groupBy("userString", "keyWord").agg(sum("keyWordsRating")).drop("keyWordsRating").
      withColumnRenamed("sum(keyWordsRating)", "keyWordsRating")

    //(userString: String, rowkey: String, value: Double, label: String, labelValue: Double)
    //(rowkey: String, keyWord: String, keyWordsValue: Double)
    val (kMin, kMax) = keywordDS2.agg(min($"keyWordsRating"), max($"keyWordsRating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val k_Normalized = ($"keyWordsRating" - kMin) / (kMax - kMin) // v normalized to (0, 1) range
    val keyword_Scaled = scaledRange * k_Normalized + scaledMin
    val keywordDS3 = keywordDS2.withColumn("keyWordsRating", bround(keyword_Scaled, 4)).withColumnRenamed("keyWord", "words")
    //userString, keyWord, keyWordsRating
    /*
    数据合并
     */
    val fulljoin1 = searchDS2.join(labelDS3, Seq("userString", "words"), "full").
      na.fill(value = 0.0, cols = Array("searchRating", "labelRating")).na.drop()
    //userString,words,searchRating, labelRating
    val fulljoin2 = fulljoin1.join(keywordDS3, Seq("userString", "words"), "full").
      na.fill(value = 0.0, cols = Array("searchRating", "labelRating", "keyWordsRating")).na.drop()
    //userString,words,searchRating, labelRating, keyWordsRating
    val totalRating = fulljoin2.withColumn("totalRating", $"searchRating" * 0.5 + $"labelRating" * 0.3 + $"keyWordsRating" * 0.2).
      drop("searchRating").drop("labelRating").drop("keyWordsRating").filter(!$"words".contains("None"))
    //userString, words, totalRating

    /*
    排序问题
     */
    val w = Window.partitionBy("userString").orderBy(col("totalRating").desc)
    val ds1 = totalRating.withColumn("rn", row_number.over(w)).where(col("rn") <= 40).drop("totalRating").drop("rn")
    //userString, words, totalRating, rn

    val top40Words = totalRating.groupBy("words").agg(sum($"totalRating")).withColumnRenamed("sum(totalRating)", "sumRating").
      orderBy(-$"sumRating").take(40).map{case Row(words: String, rating: Double) => (words)}.mkString(";")


    /*
获取当前时间
 */
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 2
    cal.add(Calendar.DATE, -N) //获取N天前或N天后的时间，-2为2天前
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    //    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    /*
    分组合并问题
     */

    val rdd1 = ds1.rdd.map { case Row(userString: String, words: String) => (userString, words) }.reduceByKey(_ + ";" + _).
      map(x => {
        val userString = x._1.toString
        val solrWords = x._2.toString
        val time = today
        solrWordsView(userString, solrWords, time)
      })
    val ds2 = spark.createDataset(rdd1)

    /*
    将结果保存到mysql数据库
     */

    //将ds2保存到YLZX_OPER_INTEREST表中
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")

    //清空YLZX_OPER_INTEREST表
    //  truncateMysql.truncateMysqlTable("jdbc:mysql://192.168.37.102:3306/ylzx", "ylzx", "ylzx","YLZX_OPER_INTEREST")

    //将结果保存到数据框中
//    ds2.write.mode("append").jdbc(url2, "YLZX_OPER_INTEREST", prop2) //overwrite

    val ds3 = spark.createDataset(sc.parallelize(Seq(solrWordsView("top", top40Words, today))))
//    ds3.write.mode("append").jdbc(url2, "YLZX_OPER_INTEREST", prop2) //overwrite

    val ds4 = ds2.union(ds3)
    ds4.write.mode("append").jdbc(url2, "YLZX_OPER_INTEREST", prop2) //overwrite
    //删除YLZX_OPER_INTEREST表中一天前的数据
    //    deleteMysqlData.deleteMysqlData("jdbc:mysql://192.168.37.102:3306/ylzx", "ylzx", "ylzx", "YLZX_OPER_INTEREST", "2017-06-26 10:15:15")
    deleteMysqlData.deleteMysqlData("jdbc:mysql://192.168.37.102:3306/ylzx", "ylzx", "ylzx", "YLZX_OPER_INTEREST", nDaysAgo)

    sc.stop()
    spark.stop()
  }

}
