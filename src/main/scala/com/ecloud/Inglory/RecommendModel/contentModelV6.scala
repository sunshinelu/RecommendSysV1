package com.ecloud.Inglory.RecommendModel

import java.text.SimpleDateFormat
import java.util.Date

import com.ecloud.Inglory.Solr.AddIndex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, ResultScanner, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/11/6.
 * contentModelV5与contentModelV6的区别在于：
 * contentModelV6中关于对simsScore的处理进行了修正
 */
object contentModelV6 {
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

  case class DocsimiSchema(id: String, simsID: String, simsScore: Double, level: Double, title: String, manuallabel: String, mod: String, websitename: String)

  def getDocsimiData(tableName: String, sc: SparkContext): RDD[DocsimiSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
      val simsID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
      val simsScore = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore
      val level = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("t")) //title
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
      val mod = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
      val websitename = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //mod
      (id, simsID, simsScore, level, title, manuallabel, mod, websitename)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6 & null != x._7 & null != x._8).
      map(x => {
        val id = Bytes.toString(x._1)
        val simsID = Bytes.toString(x._2)
        val simsScore = Bytes.toString(x._3).toDouble
        val simsScore2 = (-1 * simsScore) + 1.0
        val level = Bytes.toString(x._4)
        val level2 = (-0.1 * level.toInt) + 1
        val title = Bytes.toString(x._5)
        val manuallabel = Bytes.toString(x._6)
        val mod = Bytes.toString(x._7)
        val websitename = Bytes.toString(x._8)
        DocsimiSchema(id, simsID, simsScore2, level2, title, manuallabel, mod, websitename)
      })
    hbaseRDD
  }


  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"ylzx_cnxh: contentModelV6") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)


    val ylzxTable = args(0)
    val logsTable = args(1)
    val docsimiTable = args(2)
    /*
 val ylzxTable = "yilan-total-analysis_webpage"
 val logsTable = "t_hbaseSink"
 val docsimiTable = "ylzx_xgwz"
  val outputTable = "ylzx_cnxh"
 */
    val outputTable = args(3)

    val ylzxRDD = RecommendModelUtil.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content").dropDuplicates()

    val logsRDD = RecommendModelUtil.getLogsRDDV2(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))

    // 对logsDS中的value列进行标准化处理
    val vectorizeCol = udf((v: Double) => Vectors.dense(Array(v)))
    val logsDS_vec = logsDS.withColumn("vVec", vectorizeCol(col("value")))
    val logsDS_scaler = new MinMaxScaler()
      .setInputCol("vVec")
      .setOutputCol("vScaled")
      .setMax(1)
      .setMin(0)

    val logsDS_norm = logsDS_scaler.fit(logsDS_vec).transform(logsDS_vec)
    val reversVectorizeCol = udf { (v: DenseVector) => v(0).toString.toDouble }
    val logsDS_norm2 = logsDS_norm.drop("value").drop("vVec").
      withColumn("vScaled2", reversVectorizeCol(col("vScaled")))

    val df1 = logsDS_norm2.select("userString", "itemString", "vScaled2").withColumnRenamed("vScaled2", "value")
    val df1_1 = logsDS_norm2.select("userString", "itemString")


    val docsimiRDD = getDocsimiData(docsimiTable, sc)
    val docsimiDS = spark.createDataset(docsimiRDD).filter($"simsScore" >= 0.2) //.drop("simsScore")

    //使用info:simsScore列作为权重，代替info:level列。
    val df2 = docsimiDS.select("id", "simsID", "simsScore").withColumn("score", bround(($"simsScore" * (-1) + 1) * 5, 3)).
      drop("simsScore")

    // 关于score和value的权重问题
    val df3 = df1.join(df2, df1("itemString") === df2("id"), "left").
      withColumn("rating", col("value") * col("score")).drop("value").drop("score").na.drop()


    val df4 = df3.drop("itemString").drop("id").withColumnRenamed("simsID", "itemString").
      join(df1_1, Seq("userString", "itemString"), "leftanti").na.drop().
      groupBy("userString", "itemString").agg(sum($"rating")).drop("rating").withColumnRenamed("sum(rating)", "rating")

    val df5 = df4.join(ylzxDS, Seq("itemString"), "left").na.drop()


    // 根据userString进行分组，对打分进行倒序排序，获取打分前10的数据。
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val df6 = df5.withColumn("rn", row_number.over(w)).where($"rn" <= 70) //70

    val df7 = df6.select("userString", "itemString", "rating", "rn", "title", "manuallabel", "time")

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //如果outputTable表存在，则删除表；如果不存在则新建表。

    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    df7.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6))).
      map(x => {
        val userString = x._1.toString
        val itemString = x._2.toString
        //保留rating有效数字
        val rating = x._3.toString.toDouble
        val rating2 = f"$rating%1.5f".toString
        val rn = x._4.toString
        val title = if (null != x._5) x._5.toString else ""
        val manuallabel = if (null != x._6) x._6.toString else ""
        val time = if (null != x._7) x._7.toString else ""
        val sysTime = today
        (userString, itemString, rating2, rn, title, manuallabel, time, sysTime)
      }).filter(_._5.length >= 2).
      map { x => {
        val paste = x._1 + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,userID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._2.toString)) //id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(x._3.toString)) //rating
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rn"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(x._5.toString)) //title
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //manuallabel
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //mod
        put.add(Bytes.toBytes("info"), Bytes.toBytes("sysTime"), Bytes.toBytes(x._8.toString)) //sysTime

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf) //.saveAsNewAPIHadoopDataset(job.getConfiguration)

    println("save hbase table succeed!")

    val solrUrl_guessYouLikeRec: String = "http://192.168.37.11:8983/solr/solr-yilan-guessYouLikeRec"
    //    val tableName_guessYouLike: String = "ylzx_cnxh"
    val results_guessYouLike: ResultScanner = AddIndex.getAllRows(outputTable)
    AddIndex.purgAllIndex(solrUrl_guessYouLikeRec)
    AddIndex.addIndex_GuessYouLike(results_guessYouLike, solrUrl_guessYouLikeRec)

    println("build index succeed!!")


    sc.stop()
    spark.stop()

  }

}
