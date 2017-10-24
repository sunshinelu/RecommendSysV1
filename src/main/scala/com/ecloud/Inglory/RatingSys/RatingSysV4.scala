package com.ecloud.Inglory.RatingSys

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ecloud.Inglory.RatingSys.RatingSysV1.{Schema2, LogView, LogView2, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{TableName, HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/5/16.
 *  * 构建评分系统：
 * 衰减因子r、行为权重
 *
 * 在RatingSysV1的基础上，对浏览、点赞、收藏、取消收藏的数据赋予不同的打分。
 * 在RatingSysV2的基础上对时间进行过滤，删除20年前的数据。并在输出结果中保存当前系统时间
 * 在RatingSysV3的基础上对易览资讯数据内容进行过滤。（因为在推荐的结果中存在内容相同的数据）
 *
 *
 *
 * args(0):易览资讯数据所在HBASE表
 * args(1):日志所在HBASE表
 * args(2)：输出HBASE表
 */
object RatingSysV4 {

  case class Schema(itemString: String, title: String, manuallabel: String, time: String, websitename: String, content: String)


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getYlzxRDD(ylzxTable:String, sc:SparkContext):RDD[Schema] = {
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 10
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
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))//time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename"))//websitename
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content
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
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //websitename列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content列
      (urlID, title, manuallabel, time, webName, content)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if(null != x._2) Bytes.toString(x._2) else ""
        val manuallabel_1 = if(null != x._3)  Bytes.toString(x._3) else ""
        //时间格式转化
        val time = Bytes.toLong(x._4)

        val websitename_1 = if(null != x._5) Bytes.toString(x._5) else ""
        val content_1 = Bytes.toString(x._6)
        (urlID_1, title_1, manuallabel_1, time, websitename_1, content_1)
      }
      }.filter(x => {x._2.length >= 2}).filter(x => x._4 >= nDaysAgoL).map(x => {
      val date: Date = new Date(x._4)
      val time = dateFormat.format(date)
      val content = x._6.replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
      Schema(x._1, x._2, x._3, time, x._5, content)
    })

    hbaseRDD

  }

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

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
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
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
          case r if (r.contains("getContentById.do")) => 0.2 * value
          case r if (r.contains("like/add.do")) => 0.3 * value
          case r if (r.contains("favorite/add.do")) => 0.5 * value
          case r if (r.contains("addFavorite.do")) => 0.5 * value //0.5
          case r if (r.contains("favorite/delete.do")) => -0.5 * value
          case r if (r.contains("delFavorite.do")) => -0.5 * value //-0.5
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5)

    hbaseRDD
  }



  def main(args: Array[String]) {
    // build spark environment
    val spark = SparkSession.builder().appName("RatingSysV4").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)


    val ylzxRDD = getYlzxRDD(args(0), sc)
    //    val ylzxRDD = getYlzxRDD("yilan-total_webpage", sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")
    // ylzxDF.persist()

    val logsTable = args(1)

    val logsRDD = getLogsRDD(logsTable, sc)
    //    val logsRDD = getLogsRDD("t_hbaseSink", sc)
    val logsRDD2 = logsRDD.map(x => {
      val userString = x.userString
      val itemString = x.itemString
      val time = x.CREATE_TIME
      val value = x.value

      val rating = time match {
        case x if(x >= UtilTool.get3Dasys())  => 0.9 * value
        case x if(x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys())  => 0.8 * value
        case x if(x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys())  => 0.7 * value
        case x if(x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth())  => 0.6 * value
        case x if(x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth())  => 0.5 * value
        case x if(x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth())  => 0.4 * value
        case x if(x < UtilTool.getOneYear())  => 0.3 * value
        case _ => 0.0
      }

      //val rating = rValue(time, value)
      LogView2(userString, itemString, time, rating)
    })


    val logsDS = spark.createDataset(logsRDD2).na.drop(Array("userString"))
    val ds1 = logsDS.groupBy("userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")

    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)

    val ds4 = ds3.withColumn("userID", ds3("userID").cast("long")).
      withColumn("urlID", ds3("urlID").cast("long")).
      withColumn("rating", ds3("rating").cast("double"))


    //Min-Max Normalization[-1,1]
    val minMax = ds4.agg(max("rating"), min("rating")).withColumnRenamed("max(rating)", "max").withColumnRenamed("min(rating)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first()
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val ds5 = ds4.withColumn("norm", bround((((ds4("rating") - minValue) / (maxValue - minValue)) * 2 - 1), 4))

    //RDD to RowRDD
    val alsRDD = ds5.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toInt
      val item = x._2.toString.toInt
      val rate = x._3.toString.toDouble
      Rating(user, item, rate)
    }

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(alsRDD, rank, numIterations, 0.01)

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


    val joinDF1 = topProductsDF.join(userLab, Seq("userID"), "left")
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left")
    val joinDF3 = joinDF2.join(ylzxDF, Seq("itemString"), "left").na.drop()
    ylzxDF.unpersist()
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val joinDF4 = joinDF3.withColumn("rn", row_number.over(w)) //.where($"rn" <= 10)
    val joinDF5 = joinDF4.select("userString", "itemString", "rating", "rn", "title", "manuallabel", "time")


    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //如果outputTable表存在，则删除表；如果不存在则新建表。
    val outputTable = args(2)
    //    val outputTable = "t_RatingSys"
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
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
        val sysTime = today
        (userString, itemString, rating2, rn, title, manuallabel, time, sysTime )
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
      }.saveAsNewAPIHadoopDataset(jobConf)//.saveAsNewAPIHadoopDataset(job.getConfiguration)



    sc.stop()
    spark.stop()
  }


}
