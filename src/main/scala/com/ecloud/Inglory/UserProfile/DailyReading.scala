package com.ecloud.Inglory.UserProfile

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.ansj.app.keyword.KeyWordComputer
import org.ansj.library.UserDefineLibrary
import org.ansj.recognition.NatureRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
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
 * Created by sunlu on 17/10/12.
 * 1、对所有用户当天/前天阅读的文章分别进行关键词提取，根据词性进行过滤，只保留名词。
 * 每篇文章至多保存5个关键词，然后对关键词进行wordcount，取top 50～100.
 * 2、对所有用户当天/前天阅读的文章所在的行政区划（p:xzqhname）进行wordcount。
 * 最终结果为：
 * 主键、时间（年月日）、关键词（词：权重；词：权重）、区域（行政编号a：权重；行政编号b：权重）、创建时间（年月日时分秒）
 *
 *
 * 测试：
 * spark-shell --master yarn --num-executors 4 --executor-cores  2 --executor-memory 4g --jars /root/software/extraClass/solr-solrj-5.1.0.jar,/root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar
 *
 *
 * CREATE TABLE `YLZX_TJ_MT_YHXW` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `RMCTJ` text COMMENT '热门词统计前50',
  `XZQHTJ` text COMMENT '行政区划统计前二十',
  `WZLBTJ` text COMMENT '网站类别统计前十',
  `WZTJ` varchar(255) DEFAULT NULL COMMENT '用户访问的前十网站',
  `SSSJ`：时间,
  `CJSJ` varchar(19) DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='每一天所有用户的用户行为统计表';
 *
任务运行命令：

spark-submit \
--class com.ecloud.Inglory.UserProfile.DailyReading \
--master yarn \
--num-executors 4 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/ylzx/RecommendSysV1.jar \
yilan-total-analysis_webpage t_hbaseSink

 *
 *
 */
object DailyReading {

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

  case class DailyReadingSchema(SSSJ:String, RMCTJ:String,XZQHTJ:String)//time:String, keywordsList:String,distList:String

  def getNDaysAgo(timeFormat:String,N:Int): String = {
    // 获取时间
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat(timeFormat) // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    //        cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
//    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime
    nDaysAgo
  }
  def getNDaysAgoL(timeFormat:String,N:Int): Long = {
    // 获取时间
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat(timeFormat) // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    //        cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime
    nDaysAgoL
  }

  case class LogView(CREATE_BY_ID: String, CREATE_TIME_L: Long, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: String, value: Double)


  def getDailyLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

    // 获取时间
    val nDaysAgoL = getNDaysAgoL("yyyy-MM-dd",1)


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
    ).filter(_.CREATE_TIME_L == nDaysAgoL).
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


  case class DailyYlzxSchema(itemString: String, title: String, content: String, dist: String)

  def getDailyYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[DailyYlzxSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容
      val dist = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划
      (urlID, title, content, dist)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val dist_1 = Bytes.toString(x._4)
        DailyYlzxSchema(urlID_1, title_1, content_1, dist_1)
      }
      )
    hbaseRDD
  }


  def main(args: Array[String]) {

    /*
    //在用词典未加载前可以通过,代码方式方式来加载
    MyStaticValue.userLibrary = "/personal/sunlu/ylzx/userDefine.dic"
    UserDefineLibrary.insertWord("大数据", "userDefine", 1000)
    UserDefineLibrary.insertWord("解决方法", "userDefine", 1000)
    UserDefineLibrary.removeWord("大数据")
*/
    // 不输出日志
    SetLogger

    /*
    1. bulid spark environment
     */

    val sparkConf = new SparkConf().setAppName(s"DailyReading_YLZX_TJ_MT_YHXW") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    2. get data
     */

    val ylzxTable = args(0)
    val logsTable = args(1)

    /*
        val ylzxTable = "yilan-total-analysis_webpage"
        val logsTable = "t_hbaseSink"
    */

    // 加载词典
    val userDefineFile= "/personal/sunlu/ylzx/userDefine.dic"
    val userDefineList = sc.textFile(userDefineFile).collect().toList
    userDefineList.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })
    MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171023.txt"// bigdata7路径

    // 获取日志数据
    val logsRDD = getDailyLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).groupBy("itemString").agg(sum("value")).drop("value").
      withColumnRenamed("sum(value)", "value")

    // 获取易览资讯数据
    val ylzxRDD = getDailyYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)

    // join logsDS and ylzxDS
    val df  = logsDS.join(ylzxDS, Seq("itemString"), "left")

    //定义UDF
    //关键词提取
    def getKeyWordsFunc(title: String, content: String): String = {
      //每篇文章提取5个关键词
      val kwc = new KeyWordComputer(5)
      val keywords = kwc.computeArticleTfidf(title, content).toString.replace("[","").replace("]","")

      val result = keywords match {
        case r if (r.length >= 2) => r
        case _ => "NULL" // Seq("null")
      }
      result
    }
    val KeyWordsUDF = udf((title: String, content: String) => getKeyWordsFunc(title, content))

    // get keywords based on title and content
    val df1 = df.withColumn("getKW", KeyWordsUDF($"title", $"content")).
      drop("content").drop("title").
      filter($"getKW" =!= "NULL")

    //split $"getKW" using explode and split functions
    val df2 = df1.withColumn("KwW",explode(split($"getKW", ",")))

    // get keywords and it's weight
    val df3 = df2.withColumn("words_temp", split($"KwW", "/")(0)).
      withColumn("words", regexp_replace($"words_temp", " ", "")).drop("words_temp").
      withColumn("weight",split($"KwW", "/")(1)).
      drop("getKW").drop("KwW").
      withColumn("weight", col("weight").cast("double"))

    // reorder $"weight"
    val w = Window.partitionBy($"itemString").orderBy($"weight".asc)
    val df4 = df3.withColumn("rn", row_number.over(w)).drop("weight")

    // get rating
    val df5 = df4.withColumn("rating", $"rn" * $"value").drop("rn")


    //获取词性
    def getNaturesFunc(content: String): String = {
      val terms = ToAnalysis.parse(content)
      new NatureRecognition(terms).recognition()
      val natures = terms.toString.replace("[", "").replace("]", "").split("/")(1)//.mkString("")
      val result = natures match {
          case r if (! r.contains(",")) => r
          case _ => "n"
        }
      result
    }
    val NaturesUDF = udf((content: String) => getNaturesFunc(content))


    val ColumnsName = Seq("words","words2")
    val countryDF = spark.read.option("header", true).option("delimiter", ",").
      csv("/personal/sunlu/ylzx/country.csv").toDF(ColumnsName:_*)

    val keywordsDF_temp = df5.select("words", "rating")


    def exchangeFunc(arg1:String,arg2:String):String = {
      val result = arg2 match {
        case r if r!= null => arg2
        case _ => arg1
      }
      result
    }
    val exchangeUDF = udf((arg1:String,arg2:String) => exchangeFunc(arg1,arg2))
    val keywordsDF_temp2 = keywordsDF_temp.join(countryDF, Seq("words"), "left").
      withColumn("words", exchangeUDF($"words", $"words2")).drop("words2")


    val w1 = Window.partitionBy($"words").orderBy($"v".desc)
    val keywordsDF = keywordsDF_temp2.groupBy("words").agg(sum($"rating")).
      withColumnRenamed("sum(rating)", "v").withColumn("nature", NaturesUDF($"words")).
      filter($"nature".contains("n")).
      withColumn("rn", row_number.over(w1)).filter($"rn" <= 50).drop("rn")

    val w2 = Window.partitionBy($"dist").orderBy($"v".desc)
    val distDF = df5.groupBy("dist").agg(sum($"value")).withColumnRenamed("sum(value)", "v").
      withColumn("rn", row_number.over(w2)).filter($"rn" <= 20).drop("rn")


    /*
    Normaliz $"v"
     */
    val scaledRange = lit(1) // Range of the scaled variable
    val scaledMin = lit(0) // Min value of the scaled variable

    val (kMin, kMax) = keywordsDF.agg(min($"v"), max($"v")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val kNormalized = ($"v" - kMin) / (kMax - kMin) // v normalized to (0, 1) range
    val kScaled = scaledRange * kNormalized + scaledMin
    val keywordsDF2 = keywordsDF.withColumn("vScaled", kScaled).withColumn("vScaled", bround($"vScaled", 3)).
      orderBy($"vScaled".desc)

//    keywordsDF2.filter($"words".contains("济南")).show(false)
//    keywordsDF2.filter($"words".contains("数据")).show(false)

    val (dMin, dMax) = distDF.agg(min($"v"), max($"v")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val dNormalized = ($"v" - dMin) / (dMax - dMin) // v normalized to (0, 1) range
    val dScaled = scaledRange * dNormalized + scaledMin
    val distDF2 = distDF.withColumn("vScaled", dScaled).withColumn("vScaled", bround($"vScaled", 3)).
      orderBy($"vScaled".desc)

    val keyworsString = keywordsDF2.select("words","vScaled").rdd.map{case Row(word:String,weight:Double) => (word, weight)}.map(x =>
    {
      val result = x._1 + ":" + x._2.toString
      (result)
    }).collect().mkString(";")

    val distString = distDF2.select("dist","vScaled").rdd.map{case Row(dist:String,weight:Double) => (dist, weight)}.map(x =>
    {
      val result = x._1 + ":" + x._2.toString
      (result)
    }).collect().mkString(";")

    val yesterday = getNDaysAgo("yyyy-MM-dd", 1)

    val resultDF = spark.createDataset(sc.parallelize(Seq(DailyReadingSchema(yesterday,keyworsString,distString)))).
      withColumn("CJSJ", current_timestamp()).withColumn("CJSJ", date_format($"CJSJ", "yyyy-MM-dd HH:mm:ss"))



    //将joinedDf保存到result表中
    val resultTable = "YLZX_TJ_MT_YHXW"
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")
    resultDF.write.mode("append").jdbc(url2, resultTable, prop2)

    sc.stop()
    spark.stop()

  }

}
