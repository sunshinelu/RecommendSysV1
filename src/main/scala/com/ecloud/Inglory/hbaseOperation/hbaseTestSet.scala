package com.ecloud.Inglory.hbaseOperation

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/5/20.
 */
object hbaseTestSet {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("hbaseOperation: hbaseTestSet").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._






  }
}
