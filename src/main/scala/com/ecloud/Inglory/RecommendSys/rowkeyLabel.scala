package com.ecloud.Inglory.RecommendSys

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/3/6.
 * 读取hbase数据库中的rowkey列和label列，然后进行数据清洗，清洗后数据格式如下：
 * label:rowkey1;rowkey2.....
 *
 */
object rowkeyLabel {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("rowkeyLabel").getOrCreate()
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yilan-ywk_webpage
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
    //scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))//time
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      (urlID, label)
    }
    }.filter(x => null != x._2).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val label_1 = if(null != x._2)  Bytes.toString(x._2) else ""
        (urlID_1, label_1)
      }
      }.filter(x => x._2.length >= 2 )
    val rdd1 = hbaseRDD.map(x => {
      val x2 = x._2.split(";")
      (x._1, x2.toIterable)
    }
    ).flatMap(x => {
      val y = x._2
      for (w <- y) yield (x._1, w)
    })
    val rdd2 = rdd1.map(_.swap).reduceByKey(_ + ";" + _)
    /*
    rdd2.map { x => {
      val key = Bytes.toBytes(x._1)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("ids"), Bytes.toBytes(x._2.toString)) //标签的family:qualify
      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)
*/
    val rowRDD = rdd2.map(x => Row(x._1, x._2))

    // make schema
    val schema = StructType(
      StructField("manuallabel", StringType) ::
        StructField("ids", StringType)
        :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)

    //将df保存到label_ids_Test表中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    df.write.mode("overwrite").jdbc(url2, "label_ids_Test", prop2)

    sc.stop()


  }
}
