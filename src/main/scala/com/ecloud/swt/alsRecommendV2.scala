package com.ecloud.swt

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.ecloud.Inglory.RatingSys.UtilTool
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/16.
 * 推荐的数据中只推荐最新数据，时间在一年以内
 * 方法一：在读取用户行为日志时，根据衰减因子对数据进行过滤
 *  存在问题：由于缩减建模数据导致无建模结果
 * 方法二：读取易览资讯数据时对时间进行过滤
 *
 */
object alsRecommendV2 {


  case class ylzxSchema(itemString: String, title: String, manuallabel: String, time: Long)

  case class logsSchema(operatorId: String, userString: String, itemString: String, accessTime: String, accessTimeL: Long, value:Double)
  case class recommSchema(userID: Long, urlID: Long, rating: Double)
  case class mysqlSchema(USERNAME: String, OPERATOR_ID: String, ATICLEID: String ,TITLE: String, ATICLETIME: String, CREATETIME: String, RATE: Int)

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[ylzxSchema] = {

    // 获取时间
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
    val N = 1
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
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, title, manuallabel, time)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val manuallabel_1 = Bytes.toString(x._3)
        //时间格式转化
        val time = Bytes.toLong(x._4)//toString(x._4).toLong
        ylzxSchema(urlID_1, title_1, manuallabel_1, time)
      }
      }.filter(x => {
      x.title.length >= 2
    }).filter(x => x.time >= nDaysAgoL).filter(x => x.manuallabel.contains("商务"))

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
        logsSchema(r1, r2, r3, r4.toString, dateFormat2.parse(r4.toString).getTime, 1.0) }

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

      logsSchema(x.operatorId, x.userString, x.itemString, x.accessTime,x.accessTimeL,rating)
    })//.filter(_.value >= 0.4)

    rdd2
  }


  def main(args: Array[String]) {
    // build spark environment
    val spark = SparkSession.builder().appName("shangwuting: alsRecommendV2").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)


    val ylzxRDD = getYlzxRDD(args(0), sc)
    //    val ylzxRDD = getYlzxRDD("yilan-total_webpage", sc)
    val ylzxDF = spark.createDataset(ylzxRDD)
    // ylzxDF.persist()

    val logsTable = args(1)

    val logsRDD = getLogsRDD(logsTable, spark, sc)
    //    val logsRDD = getLogsRDD("SPEC_LOG_CLICK", spark, sc)


    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString")).filter($"operatorId" === "7a4e4f92-7357-4057-8b39-7f5f96f341c2")

    val ds1 = logsDS.groupBy("operatorId","userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")

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
    val ds5 = ds4.withColumn("norm", bround((((ds4("rating") - minValue) / (maxValue - minValue)) * 2 - 1), 4)).na.drop()

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

    val topProducts = model.recommendProductsForUsers(15)

    val topProductsRowRDD = topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map { f => recommSchema(f._1.toLong, f._2.toLong, f._3) }

    val topProductsDF = spark.createDataset(topProductsRowRDD)

    val userLab = ds5.select("operatorId","userString", "userID").dropDuplicates
    val itemLab = ds5.select("itemString", "urlID").dropDuplicates


    val joinDF1 = topProductsDF.join(userLab, Seq("userID"), "left")
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left")
    val joinDF3 = joinDF2.join(ylzxDF, Seq("itemString"), "left").na.drop()
    ylzxDF.unpersist()
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val joinDF4 = joinDF3.withColumn("rn", row_number.over(w)).where($"rn" <= 6)

    val addSysTime = udf((userString:String) =>{
      today
    })

    val joinDF5 = joinDF4.withColumn("CREATETIME", addSysTime(col("UserString"))).select("userString", "operatorId","itemString", "title", "time", "CREATETIME","rn")

    val columnsRenamed = Seq("USERNAME", "OPERATOR_ID", "ATICLEID" ,"TITLE", "ATICLETIME", "CREATETIME", "RATE")
    val df = joinDF5.toDF(columnsRenamed: _*)

    df.createTempView("df1")
    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")
    //清空SPEC_LOG_RECOM表
    alsRecommend.truncateMysql("jdbc:mysql://192.168.37.102:3306/ylzx", "ylzx", "ylzx", "SPEC_LOG_RECOM")
    //将结果保存到数据框中
    df.write.mode("append").jdbc(url2, "SPEC_LOG_RECOM", prop2)//overwrite or append


    sc.stop()
    spark.stop()


  }

  def truncateMysql(url: String, user: String, password:String, tableName:String) : Unit ={
    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"
    // 加载驱动程序
    Class.forName(driver)
    // 连续数据库
    val conn = DriverManager.getConnection(url, user, password)
    if (!conn.isClosed())
      System.out.println("Succeeded connecting to the Database!")
    // statement用来执行SQL语句
    val statement = conn.createStatement()
    // 要执行的SQL语句
    val sql = "truncate table " + tableName

    val rs = statement.executeUpdate(sql)
    println("truncate table succeeded!")
  }

}
