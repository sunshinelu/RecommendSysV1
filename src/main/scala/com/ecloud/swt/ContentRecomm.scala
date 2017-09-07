package com.ecloud.swt

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.ecloud.Inglory.RatingSys.UtilTool
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunlu on 17/9/6.
  * 使用基于内容的推荐，构建推荐模型：
  * ** 使用tf-idf生成文章向量，余弦计算文章相似性
  * ** 然后使用item-based方法完成推荐
  * 测试
  * spark-shell --master yarn --num-executors 4 --executor-cores  2 --executor-memory 4g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar
  *
  */
object ContentRecomm {

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

  case class ylzxSchema(itemString: String, title: String, segWords: Seq[String], manuallabel: String, time: Long)

  case class docSimsSchema(doc1: Long, doc2: Long, sims: Double)

  case class logsSchema(operatorId: String, userString: String, itemString: String, accessTime: String, accessTimeL: Long, value: Double)

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
      (urlID, title, content, manuallabel, time)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5).
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
        ylzxSchema(urlID_1, title_1, segWords, manuallabel_1, time)
      }
      }.filter(x => {
      x.title.length >= 2
    }).filter(x => x.time >= nDaysAgoL).filter(_.segWords.size > 1).filter(x => x.manuallabel.contains("商务"))

    hbaseRDD
  }

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


  def main(args: Array[String]) {
    // 不输出日志
    SetLogger

    /*
    1. bulid spark environment
     */

    val sparkConf = new SparkConf().setAppName(s"swt: ContentRecomm") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    2. get data
     */
    val ylzxTable = args(0)
    val logsTable = args(1)

    /*
        val ylzxTable = "yilan-total_webpage"
    val logsTable = "SPEC_LOG_CLICK"
     */

    val ylzxRDD = getYlzxRDD(ylzxTable, 1, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)

    val logsRDD = getLogsRDD(logsTable, spark, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString")).filter($"operatorId" === "7a4e4f92-7357-4057-8b39-7f5f96f341c2")
    val logsDS2 = logsDS.groupBy("operatorId", "userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")
    logsDS2.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /*
    3. content based document similarity calculation
     */
    //string to number
    val itemId = new StringIndexer().setInputCol("itemString").setOutputCol("itemID").fit(ylzxDS)
    val itemIdDf = itemId.transform(ylzxDS)

    // calculate tf-idf
    val hashingTF = new HashingTF().
      setInputCol("segWords").setOutputCol("tfFeatures") //.setNumFeatures(200000)
    val tfData = hashingTF.transform(itemIdDf)

    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("tfidfVec")
    val idfModel = idf.fit(tfData)
    val tfidfDF = idfModel.transform(tfData)

    val tfidfDF2 = tfidfDF.withColumn("itemID", $"itemID".cast("long"))
    tfidfDF2.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val document = tfidfDF2.select("itemID", "tfidfVec").na.drop.rdd.map {
      case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))
    }.filter(_._2.size >= 2) //.distinct

    val tfidf = document.map { case (i, v) => new IndexedRow(i, v) } //.repartition(50)
    val mat = new IndexedRowMatrix(tfidf)

    val mat_t = mat.toCoordinateMatrix.transpose()
    val threshhold = 0.3
    val upper = 1.0

    val sim = mat_t.toRowMatrix.columnSimilarities(threshhold)

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper } //.repartition(400)
    sim_threshhold.persist(StorageLevel.MEMORY_AND_DISK)

    val docSimsRDD1 = sim_threshhold.map { x => {
      val doc1 = x.i
      val doc2 = x.j
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }
    val docSimsRDD2 = docSimsRDD1.map { x => {
      val doc1 = x.doc2
      val doc2 = x.doc1
      val sims = x.sims
      docSimsSchema(doc1, doc2, sims)
    }
    }

    val docSimsRDD = docSimsRDD1.union(docSimsRDD2)
    val docSimisDS = spark.createDataset(docSimsRDD)

    /*
    4. item-based recommendation
     */
    val itemLab = tfidfDF2.select("itemID", "itemString")
    val itemLab2 = tfidfDF2.select("itemString", "title", "manuallabel", "time")

    val logsDS3 = logsDS2.join(itemLab, Seq("itemString"), "left").na.drop().withColumnRenamed("itemID", "doc1")

    val joinedDf1 = logsDS3.join(docSimisDS, Seq("doc1"), "left").na.drop().
      withColumn("score", col("rating") * col("sims")).
      groupBy("operatorId", "userString", "doc2").agg(sum("score")).withColumnRenamed("sum(score)", "rating")

    val logsDS4 = logsDS3.withColumnRenamed("doc1", "doc2").
      withColumn("whether", lit(1))

    val joinedDf2 = joinedDf1.join(logsDS4, Seq("operatorId", "userString", "doc2"), "left").filter(col("whether").isNull)

    val joinedDf3 = joinedDf2.join(itemLab2, Seq("itemString"), "left").drop("whether")

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("userString").orderBy(col("time").desc)
    val rankDF = joinedDf3.withColumn("rn", row_number.over(w)).where(col("rn") <= 6)

    //.select("userString", "operatorId","itemString", "title", "time", "CREATETIME","rn")
    val columnsRenamed = Seq("USERNAME", "OPERATOR_ID", "ATICLEID", "TITLE", "ATICLETIME", "CREATETIME", "RATE")
    val resultDF = rankDF.withColumn("systime", current_timestamp()).
      withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss")).
      select("userString", "operatorId", "itemString", "title", "time", "systime", "rn").
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
    resultDF.write.mode("append").jdbc(url2, "SPEC_LOG_RECOM", prop2) //overwrite or append

    sc.stop()
    spark.stop()
  }

}
