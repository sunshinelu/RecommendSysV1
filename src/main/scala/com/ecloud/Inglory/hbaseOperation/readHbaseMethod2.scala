package com.ecloud.Inglory.hbaseOperation

import jodd.util.PropertiesUtil
import org.apache.hadoop.hbase.client.Scan
//import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.JavaConversions

/**
 * Created by sunlu on 17/3/28.
 * http://www.cnblogs.com/cssdongl/p/6238007.html
 */
object readHbaseMethod2 {
  /*
  def main(args: Array[String]) {
    val tableName = "tableName"
    val sparkConf = new SparkConf().setAppName("readHbaseMethod2").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    //  val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    //conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    val scan = new Scan()
    scan.setCaching(100)
    scan.setStartRow(Bytes.toBytes("THERMALITO_3/14/14 9:20"))
    scan.setStopRow(Bytes.toBytes("THERMALITO_3/14/14 9:59"))

    val hbaseContext = new HBaseContext(sc, conf)
    val hbaseRdd = hbaseContext.hbaseScanRDD(tableName, scan)
    val rowKeyRdd = hbaseRdd.map(tuple => tuple._1)
    rowKeyRdd.foreach(key => println(Bytes.toString(key)))

    //hbaseRDD type => (RowKey, List[(columnFamily, columnQualifier, Value)])

    val resultRdd = hbaseRdd.map { case (tuple1, tuple2) =>
      val rowkey = Bytes.toString(tuple1)
      val scalaArraysList = JavaConversions.asScalaBuffer(tuple2)
      val sb = new StringBuilder
      scalaArraysList.foreach { array =>
        val result = Array(Bytes.toString(array._1), Bytes.toString(array._2), Bytes.toString(array._3))
        if (scalaArraysList.last != array) sb.append(result.mkString("_")).append("\t")
        else sb.append(result.mkString("_"))
      }
      (rowkey, sb.toString())

    }.foreach(println)

    sc.stop()


  }
  */
}
