package com.ecloud.Inglory.RecommendSys

import java.text.SimpleDateFormat
import java.util.{Properties, Date}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by sunlu on 17/3/7.
 * 读取hbase中yilan-ywk_webpage表的rowkey和manuallabel列，与用户行为日志中的id列进行合并。
 * 注意，合并后一定要剔除manuallabel列中的null值，否则会报错，空指针。
 * 对合并后的数据中的manuallabel列进行word count，并取出前20个词拼接成自字符串保存到mysql中。
 * 测试成功！
 */
object hotLabelsV2 {
  def main(args: Array[String]) {
    //
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("hotLabelsV2").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
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
        (urlID_1, manuallabel_1)
      }
        ).filter(_._2.length >= 2)

    // make schema
    val schema = StructType(
      StructField("id", StringType) ::
        StructField("manuallabel", StringType)
        :: Nil)
    val hbaseRowRDD = hbaseRDD.map(x => Row(x._1, x._2))
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
      StructField("userID", StringType) ::
        StructField("id", StringType)
        :: Nil)
    //rdd to rowRdd
    val rowRDD1 = rdd1.map(f => Row(f._1, f._2))
    //RowRDD to DF
    val rdd1df1 = spark.createDataFrame(rowRDD1, schema1)
    val rdd1df2 = rdd1df1.join(hbaseDF, Seq("id"), "left").select("userID", "id", "manuallabel").na.drop(Array("manuallabel"))//ueserID, id, manuallabel

    val rdd2 = rdd1df2.rdd.map(row => (row(0),row(1), row(2))).map(x => {
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
    }).map(x=>(x._3, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(20)

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
    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    df5.write.mode("append").jdbc(url2, "hotWords_Test", prop2)//overwrite

    sc.stop()
    spark.stop()



  }
}
