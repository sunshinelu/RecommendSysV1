package com.ecloud.Inglory.RecommendSys

import java.text.SimpleDateFormat
import java.util.{Properties, Date}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * Created by sunlu on 17/3/6.
 * 使用mysql数据库中SYS_LOG表中数据构建ALS推荐模型
 * 与HBASE表yilan-ywk_webpage中的标题、标签进行join操作，最后将结果保存到hbase中。
 * 运行成功！（标签列不乱码）
 */
object alsDataProcessedV3 extends Serializable{
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("ContentBasedRecommendV2").getOrCreate()
    val sc = spark.sparkContext

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")//yyyy-MM-dd HH:mm:ss

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))//time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename"))//

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //时间列
      (urlID, title, manuallabel, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if(null != x._2) Bytes.toString(x._2) else ""
        val manuallabel_1 = if(null != x._3)  Bytes.toString(x._3) else ""
        //时间格式转化
        val time_1 = if(null != x._4){
          val time = Bytes.toLong(x._4)
          val date: Date = new Date(time)
          val temp = dateFormat.format(date)
          temp } else ""

        val websitename_1 = if(null != x._5) Bytes.toString(x._5) else ""
        (urlID_1, title_1, manuallabel_1, time_1, websitename_1)
      }
      }.filter(_._2.length >= 2)

    // make schema
    val schema = StructType(
      StructField("itemString", StringType) ::
        StructField("title", StringType) ::
        StructField("manuallabel", StringType) ::
        StructField("time", StringType) ::
        StructField("websitename", StringType)
        :: Nil)
    val hbaseRowRDD = hbaseRDD.map(x => Row(x._1, x._2, x._3, x._4, x._5))
    val hbaseDF = spark.createDataFrame(hbaseRowRDD, schema)
    hbaseDF.persist()

    //connect mysql database
    val url1 = "jdbc:mysql://192.168.37.18:3306/recommender_test"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val df1 = spark.read.jdbc(url1, "SYS_LOG", prop1)

    val df2 = df1.select("CREATE_BY", "CREATE_BY_ID", "REQUEST_URI", "PARAMS")
    val df3 = df2.na.drop(Array("CREATE_BY_ID"))
    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val df4 = df3.filter(col("REQUEST_URI").contains("search/getContentById.do"))

    val rdd1 = df4.rdd.map { row => (row(0), row(1), row(2), row(3)) }.filter(_._4.toString.length >= 10).map(x => {
      val userID = x._2.toString
      val reg2 = """id=(\w+\.){2}\w+.*,""".r
      val urlString = reg2.findFirstIn(x._4.toString).toString.replace("Some(id=", "").replace(",)", "")
      (userID, urlString)
    }).filter(_._2.length >= 10)

    // make schema1
    val schema1 = StructType(
      StructField("userString", StringType) ::
        StructField("itemString", StringType)
        :: Nil)
    //rdd to rowRdd
    val rowRDD1 = rdd1.map(f => Row(f._1, f._2))
    //RowRDD to DF
    val rdd1df1 = spark.createDataFrame(rowRDD1, schema1)

    val rdd1df2 = rdd1df1.groupBy("userString", "itemString").agg(count("userString")).withColumnRenamed("count(userString)", "value")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(rdd1df2)
    val rdd1df3 = userID.transform(rdd1df2)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(rdd1df3)
    val rdd1df4 = urlID.transform(rdd1df3)
    //change data type
    val rdd1df5 = rdd1df4.withColumn("userID", rdd1df4("userID").cast("long")).
      withColumn("urlID", rdd1df4("urlID").cast("long")).
      withColumn("value", rdd1df4("value").cast("double"))
    //Min-Max Normalization[-1,1]
    val minMax = rdd1df5.agg(max("value"), min("value")).withColumnRenamed("max(value)", "max").withColumnRenamed("min(value)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val rdd1df6 = rdd1df5.withColumn("norm", bround((((rdd1df5("value") - minValue) / (maxValue - minValue)) * 2 - 1), 4))
    //RDD to RowRDD
    val rdd3 = rdd1df6.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toInt
      val item = x._2.toString.toInt
      val rate = x._3.toString.toDouble
      Rating(user, item, rate)
    }
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(rdd3, rank, numIterations, 0.01)
    //防止路径下该文件夹存在
    val filepath = "/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true)
    } catch {
      case _: Throwable => {}
    }

    // Save and load model
    model.save(sc, "/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS") //没有overwrite功能

    val sameModel = MatrixFactorizationModel.load(sc, "/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS")

    val topProducts = model.recommendProductsForUsers(15)

    // make schema
    val schema2 = StructType(
      StructField("userID", LongType) ::
        StructField("urlID", LongType) ::
        StructField("rating", DoubleType)
        :: Nil)
    //RDD to RowRDD
    val topProductsRowRDD = topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map { f => Row(f._1.toLong, f._2.toLong, f._3) }

    //RowRDD to DF
    val topProductsDF = spark.createDataFrame(topProductsRowRDD, schema2)

    val userLab = rdd1df6.select("userString", "userID").dropDuplicates
    val itemLab = rdd1df6.select("itemString", "urlID").dropDuplicates

    val joinDF1 = topProductsDF.join(userLab, Seq("userID"), "left")
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left")
    val joinDF3 = joinDF2.join(hbaseDF, Seq("itemString"), "left")
    hbaseDF.unpersist()
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val joinDF4 = joinDF3.withColumn("rn", row_number.over(w)) //.where($"rn" <= 10)
    val joinDF5 = joinDF4.select("userString", "itemString", "rating", "rn", "title", "manuallabel", "time")

    joinDF5.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6))).
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
        (userString, itemString, rating2, rn, title, manuallabel, time)
      }).
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

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()

  }
}
