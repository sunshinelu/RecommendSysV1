package com.ecloud.Inglory.hbaseOperation


import jodd.util.PropertiesUtil
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableOutputCommitter, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/3/28.
 * http://www.cnblogs.com/cssdongl/p/6238007.html
 */
object readHbaseMethod1 {
  /*
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("readHbaseMethod1").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val zookeeper = PropertiesUtil.getValue("ZOOKEEPER_ADDRESS")
    conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)
    conf.set(TableInputFormat.INPUT_TABLE, "tableName")
    conf.set(TableInputFormat.SCAN_ROW_START,"COHUTTA_3/10/14")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "COHUTTA_3/11/14")
    conf.set(TableInputFormat.SCAN_COLUMNS, "data:psi")

    // load an RDD of (ImmutableBytesWritable, Result) tuples from the hbase table
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    val rowKeyRDD = hBaseRDD.map(tuple => tuple._1).map(item => Bytes.toString(item.get()))
    rowKeyRDD.take(3).foreach(println)

    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    resultRDD.count()

    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow).split(" ")(0), Bytes.toDouble(result.value)))
    keyValueRDD.take(3).foreach(kv => println(kv))

    // group by rowkey, get statistics for column value
    val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => StatCounter(list))
    keyStatsRDD.take(5).foreach(println)

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "tableName")
    keyStatsRDD.map{case (k, v) =>
    convertToPut(k, v)
    }.saveAsHadoopDataset(jobConfig)

    sc.stop()

  }
  */
}
