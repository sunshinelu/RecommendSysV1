package com.ecloud.Inglory.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/3/29.
 */
object readLogs extends Serializable {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String)
  case class Schema1(userString: String, itemString: String)
  case class Schema2(userID: Long, urlID: Long, rating: Double)

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("readLogs").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    //read log files
    val rdd = sc.textFile("/Users/sunlu/Downloads/log_2017-03-27.log").filter(null != _)

    //val rdd2 = rdd.map(_.split("\t")).filter(_.length == 11).map(line => (LogView(line(4), line(8), line(10))))

    val rdd3 = rdd.map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView(line(4), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val rdd4 = rdd3.filter(x => x.REQUEST_URI.contains("search/getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
      val userID = x.CREATE_BY_ID.toString
      val reg2 = """id=(\w+\.){2}\w+.*,""".r
      val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        Schema1(userID, urlString)
    }).filter(_.itemString.length >= 10)

    import spark.implicits._
    val ds = spark.createDataset(rdd4).as[Schema1]

    //ds.show(5)


    val ds1 = ds.groupBy("userString", "itemString").agg(count("userString")).withColumnRenamed("count(userString)", "value")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)

    val ds4 = ds3.withColumn("userID", ds3("userID").cast("long")).
      withColumn("urlID", ds3("urlID").cast("long")).
      withColumn("value", ds3("value").cast("double"))

   // ds4.show(5)
    //ds4.printSchema()

   //Min-Max Normalization[-1,1]
   val minMax = ds4.agg(max("value"), min("value")).withColumnRenamed("max(value)", "max").withColumnRenamed("min(value)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val ds5 = ds4.withColumn("norm", bround((((ds4("value") - minValue) / (maxValue - minValue)) * 2 - 1), 4))
    //RDD to RowRDD
    val rdd5 = ds5.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toInt
      val item = x._2.toString.toInt
      val rate = x._3.toString.toDouble
      Rating(user, item, rate)
    }
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(rdd5, rank, numIterations, 0.01)

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

    val topProductsRowRDD = topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map { f => Schema2(f._1.toLong, f._2.toLong, f._3) }

    val topProductsDF = spark.createDataset(topProductsRowRDD)

    val userLab = ds5.select("userString", "userID").dropDuplicates
    val itemLab = ds5.select("itemString", "urlID").dropDuplicates



    sc.stop()


  }
}
