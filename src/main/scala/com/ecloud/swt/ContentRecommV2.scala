package com.ecloud.swt


import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.ecloud.Inglory.RatingSys.UtilTool
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, MinHashLSH, StringIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunlu on 17/9/22.
 * 根据标题、时间、栏目ID对数据进行除重
  * 使用标题和5个关键词计算文章相似性
 * 只计算用户访问的文章与近一年文章的文章相似性
 *
spark-submit \
--class com.ecloud.swt.ContentRecommV2 \
--master yarn \
--num-executors 4 \
--executor-cores  4 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/ylzx/RecommendSysV1.jar \
yilan-total-analysis_webpage SPEC_LOG_CLICK
  */

object ContentRecommV2 {

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
  case class ylzxSchema(itemString: String, title: String, content: String, manuallabel: String, time: Long, columnId: String)

  def getYlzxRDD(ylzxTable: String, year: Int, sc: SparkContext): RDD[ylzxSchema] = {

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

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
    val N = year
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


    //val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("column_id"))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) // 内容列
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val column_id = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("column_id"))
      (urlID, title, content, manuallabel, time, column_id)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        //使用ansj分词
        val segWords = ToAnalysis.parse(content_1).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq

        val manuallabel_1 = Bytes.toString(x._4)
        //时间格式转化
        val time = Bytes.toLong(x._5)
        val column_id = Bytes.toString(x._6)
        ylzxSchema(urlID_1, title_1, content_1, manuallabel_1, time, column_id)
      }
      }.filter(x => {
      x.title.length >= 2
    }).filter(x => x.time >= nDaysAgoL).//filter(_.segWords.size > 1).
      filter(x => x.manuallabel.contains("商务"))

    hbaseRDD
  }
  case class logsSchema(operatorId: String, userString: String, itemString: String, accessTime: String, accessTimeL: Long, value: Double)

  def getLogsRDD(logsTable: String, spark: SparkSession, sc: SparkContext): RDD[logsSchema] = {

    //val logsTable = "SPEC_LOG_CLICK"
    //connect mysql database
    val url1 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "ylzx")
    prop1.setProperty("password", "ylzx")

    val df1 = spark.read.jdbc(url1, logsTable, prop1).drop("LOG_ID").drop("USERIP")
    /*
      OPERATOR_ID: 用户ID
      USERNAME: 用户名
      ATICLEID: 文章的ID
      ACCESSTIME: 访问时间
     */
    val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val rdd1 = df1.rdd.map { case Row(r1: String, r2: String, r3: String, r4) =>
        logsSchema(r1, r2, r3, r4.toString, dateFormat2.parse(r4.toString).getTime, 1.0)
      }

    val rdd2 = rdd1.map(x => {

      val rating = x.value match {
        case x if (x >= UtilTool.get3Dasys()) => 0.9 * x
        case x if (x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys()) => 0.8 * x
        case x if (x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys()) => 0.7 * x
        case x if (x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth()) => 0.6 * x
        case x if (x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth()) => 0.5 * x
        case x if (x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth()) => 0.4 * x
        case x if (x < UtilTool.getOneYear()) => 0.1 * x
        case _ => 0.0
      }

      logsSchema(x.operatorId, x.userString, x.itemString, x.accessTime, x.accessTimeL, rating)
    }) //.filter(_.value >= 0.4)

    rdd2
  }

  def main(args: Array[String]): Unit = {

    // 不输出日志
    SetLogger

    /*
   1. bulid spark environment
    */

    val sparkConf = new SparkConf().setAppName(s"swt_recommend_ContentRecommV2") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    2. get data
     */
    val ylzxTable = args(0)
    val logsTable = args(1)

    /*

    // val ylzxTable = "yilan-total_webpage"
        val ylzxTable = "yilan-total-analysis_webpage"
    val logsTable = "SPEC_LOG_CLICK"
     */

    val ylzxRDD = getYlzxRDD(ylzxTable, 1, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates(Array("title","time","columnId"))

    val logsRDD = getLogsRDD(logsTable, spark, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString")).
      filter($"operatorId" === "7a4e4f92-7357-4057-8b39-7f5f96f341c2")
    val logsDS2 = logsDS.groupBy("operatorId", "userString", "itemString").
      agg(sum("value")).withColumnRenamed("sum(value)", "rating")

    /*
       3. get doc id
        */
    val docId = logsDS2.select("itemString").dropDuplicates()

    val itemId = new StringIndexer().setInputCol("itemString").setOutputCol("itemID").fit(ylzxDS)
    val itemIdDf = itemId.transform(ylzxDS)

    /*
    4. seg words
     */

    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(title: String, content: String): Seq[String] = {
      //每篇文章提取5个关键词
      val kwc = new KeyWordComputer(5)
      val keywords = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.mkString(" ")
      val combinedWords = title + keywords
      val seg = ToAnalysis.parse(combinedWords).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }

    val segWordsUDF = udf((title: String, content: String) => segWordsFunc(title, content))
    val segDF = itemIdDf.withColumn("segWords", segWordsUDF($"title", $"content")).drop("content") //.filter(!$"segWords".contains("null"))

    /*
   5. calculate tf-idf value
    */
    val hashingTF = new HashingTF().
      setInputCol("segWords").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)

    /*
6. using Jaccard Distance calculate doc-doc similarity
    */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(tfidfData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(tfidfData)
    val docIdDf = docId.join(mhTransformed, Seq("itemString"), "left").na.drop()
    val docsimi_mh = mhModel.approxSimilarityJoin(docIdDf, mhTransformed, 1.0)

    //  case class ylzxSchema(itemString: String, title: String, content: String, manuallabel: String, time: Long)
// case class logsSchema(operatorId: String, userString: String, itemString: String, accessTime: String, accessTimeL: Long, value: Double)

    val colRenamed = Seq("doc1Id", "doc1", "doc2Id", "doc2", "doc2_title",
      "doc2_label", "doc2_time", "distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.itemID", "datasetA.itemString", "datasetB.itemID", "datasetB.itemString", "datasetB.title",
      "datasetB.manuallabel", "datasetB.time", "distCol").toDF(colRenamed: _*).
      filter($"doc1Id" =!= $"doc2Id").withColumn("distCol", $"distCol" * (-1) + 1)


    /*
    7. content based document similarity calculation
     */

    //val logsDS2 = logsDS.groupBy("operatorId", "userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")

    val content_df1 = logsDS2.withColumnRenamed("itemString", "doc1").join(mhSimiDF, Seq("doc1") ,"left").
      withColumn("content_rating", col("rating") * col("distCol")).drop("distCol").drop("rating").na.drop()
    val content_df2 = logsDS2.select("userString", "itemString").withColumnRenamed("itemString" ,"doc2")

    val content_df3 = content_df1.join(content_df2, Seq("userString" ,"doc2"), "leftanti").na.drop().
    groupBy("operatorId","userString", "doc2","doc2_title", "doc2_time").agg(sum($"content_rating")).drop("content_rating").
      withColumnRenamed("sum(content_rating)", "rating")


    //对dataframe进行分组排序，并取每组的前6个
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val content_df4 = content_df3.withColumn("rn", row_number.over(w)).where(col("rn") <= 15)

    val columnsRenamed = Seq("USERNAME", "OPERATOR_ID", "ATICLEID", "TITLE", "ATICLETIME", "CREATETIME", "RATE")
    val content_df5 = content_df4.withColumn("systime", current_timestamp()).
      withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss")).
      select("userString", "operatorId", "doc2", "doc2_title", "doc2_time", "systime", "rn").
      toDF(columnsRenamed: _*)

    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")
    //清空SPEC_LOG_RECOM表
    alsRecommend.truncateMysql("jdbc:mysql://192.168.37.102:3306/ylzx", "ylzx", "ylzx", "SPEC_LOG_RECOM")
    //将结果保存到数据框中
    content_df5.coalesce(1).write.mode("append").jdbc(url2, "SPEC_LOG_RECOM", prop2) //overwrite or append


    sc.stop()
    spark.stop()
  }
}
