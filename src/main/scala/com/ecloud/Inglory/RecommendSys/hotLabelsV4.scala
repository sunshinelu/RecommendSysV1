package com.ecloud.Inglory.RecommendSys

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/6/9.
 */
object hotLabelsV4 {

  case class Schema(id: String, manuallabel: String)
  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String)
  case class Schema1(userID: String, id: String)

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("hotLabelsV4").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)//yilan-total_webpage  val ylzxTable = "yilan-total_webpage"
    val logsTable = args(1)//t_hbaseSink  val logsTable = "t_hbaseSink"

    val ylzxRDD = getYlzxRDD(ylzxTable, sc) //    val ylzxRDD = getYlzxRDD("yilan-total_webpage", sc)
    val ylzxDF = spark.createDataset(ylzxRDD)

    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD)

    val rdd1 = logsDS.join(ylzxDF, Seq("id"), "left").select("userID", "id", "manuallabel").na.drop(Array("manuallabel"))//ueserID, id, manuallabel

    val rdd2 = rdd1.rdd.map(row => (row(0),row(1), row(2))).map(x => {
      val ueserID = x._1.toString
      val id = x._2.toString
      val manuallabel = x._3.toString
      (ueserID, id, manuallabel)
    }).filter(_._3.length >= 2).map(x => {
      val x3 = x._3.split(";")
      (x._1, x._2, x3.toIterable)
    }
    ).flatMap(x => {
      val y = x._3
      for (w <- y) yield (x._1,x._2, w)
    }).map(x=>(x._3, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(35)

    val rdd3 = rdd2.map(_._1).mkString(";")

    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //获取当前时间
    val now: Date = new Date()
    //对时间格式进行格式化
    val rightNow = dateFormat.format(now)
    val rdd4 = sc.parallelize(List((rightNow,rdd3)).map(x => Row(x._1, x._2)))
    val schema2 = StructType(
      StructField("time", StringType) ::
        StructField("hotWords", StringType)
        :: Nil)
    val df5 = spark.createDataFrame(rdd4, schema2)
    //将df5保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "ylzx")
    prop2.setProperty("password", "ylzx")
    //将结果保存到数据框中
    df5.write.mode("append").jdbc(url2, "YLZX_HOT_WORDS", prop2)//overwrite

    sc.stop()
    spark.stop()


  }


  def getYlzxRDD(ylzxTable:String, sc:SparkContext):RDD[Schema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")
    //指定输出格式和输出表名
    //conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label

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
      map ( x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 =  Bytes.toString(x._2)
        Schema(urlID_1, manuallabel_1)
      }
      ).filter(_.manuallabel.length >= 2)

    hbaseRDD

  }

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[Schema1] = {

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
        LogView(userID, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
//        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 = """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        Schema1(userID, urlString)
      }).filter(_.id.length >= 10)

    hbaseRDD
  }


}
