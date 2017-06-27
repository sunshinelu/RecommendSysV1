package com.ecloud.Inglory.Test


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/6/9.
 */
object checkHbaseData {

  case class Schema(id: String, manuallabel: String)

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
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

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("hotLabelsV4").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val ylzxTable = "yilan-total_webpage"
    val ylzxRDD = getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).as[(String, String)]

    val ds1ColumnsName = Seq("id", "label")
    val ds1 = ylzxDF.flatMap {
      case (id, manuallabel) => manuallabel.split(";").map((id, _))
    }.toDF(ds1ColumnsName: _*)
    ds1.select("label").dropDuplicates().show()
    /*
    +-----+
|label|
+-----+
|   商务|
|   海洋|
|   民生|
|   企业|
|  招投标|
|   科技|
|   科学|
|   政务|
| 战略资讯|
|   人才|
| 设备采购|
+-----+
     */
    val addColume = udf((args:String) => 1 )

    ds1.withColumn("value",addColume($"label")).groupBy("label").agg(sum($"value")).show()
    /*
+-----+----------+
|label|sum(value)|
+-----+----------+
|   商务|       457|
|   海洋|       191|
|   民生|         4|
|   企业|         4|
|  招投标|     28123|
|   科技|      5137|
|   科学|        14|
|   政务|      5035|
| 战略资讯|       728|
| 设备采购|         1|
|   人才|      1992|
+-----+----------+
     */

    val addColume2 = udf(() => 1 )
    ds1.withColumn("value",addColume2()).groupBy("label").agg(sum($"value")).show()
    /*
+-----+----------+
|label|sum(value)|
+-----+----------+
|   商务|       457|
|   海洋|       191|
|   民生|         4|
|   企业|         4|
|  招投标|     28123|
|   科技|      5137|
|   科学|        14|
|   政务|      5035|
| 战略资讯|       728|
|   人才|      1992|
| 设备采购|         1|
+-----+----------+
     */

    ylzxDF.withColumn("value",addColume2()).groupBy("manuallabel").agg(sum($"value")).show()

    /*
    +-----------+----------+
|manuallabel|sum(value)|
+-----------+----------+
|      政务;科技|       373|
|         商务|       451|
|      政务;商务|         6|
|        科技;|        14|
|      政务;民生|         4|
|         企业|         4|
|        招投标|     28122|
|         科技|      4535|
|      科技;海洋|       191|
|      政务;科学|        14|
|    战略资讯;科技|         8|
|         政务|      4638|
|   招投标;设备采购|         1|
|       战略资讯|       720|
|         人才|      1976|
|      人才;科技|        16|
+-----------+----------+
     */

    ylzxDF.count
    /*
     res7: Long = 41073
     */
/*
count 'yilan-total_webpage'
=> 70266
 */


  }

}
