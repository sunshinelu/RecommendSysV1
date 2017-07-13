package com.ecloud.Inglory.hbaseOperation


import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Delete, HConnectionManager, HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/5/16.
 *删除
 *
 * CREATE_BY_ID：
20cb992f-0947-4caa-961a-df9a60e60551
21feda84-f674-4368-a93c-ce27737895e5
2d2b6097-09f4-495b-a1b8-1ab2e1414472
5388f26d-d98a-4d57-981f-b0b641e99673
66652c47-e75a-4f82-886b-daa320c54a1a
6b18aacb-b2b8-4618-8abf-fec7e85005f1
91bd68b9-2cb0-4f2f-9ee1-cc2fa473829c
d5e13d39-4889-4c26-95ec-1d5fb4d8d495

 *
 *
 */

/*
object userIDrdd extends Serializable{

  def getUserId(): List[String] = {
    val spark = SparkSession.builder.appName("deleteHbaseData2").getOrCreate()
    val sc = spark.sparkContext
    val userIDrdd = sc.parallelize(Seq("20cb992f-0947-4caa-961a-df9a60e60551",
      "21feda84-f674-4368-a93c-ce27737895e5", "2d2b6097-09f4-495b-a1b8-1ab2e1414472",
      "5388f26d-d98a-4d57-981f-b0b641e99673", "66652c47-e75a-4f82-886b-daa320c54a1a",
      "6b18aacb-b2b8-4618-8abf-fec7e85005f1", "91bd68b9-2cb0-4f2f-9ee1-cc2fa473829c",
      "d5e13d39-4889-4c26-95ec-1d5fb4d8d495")).collect().toList
      userIDrdd
  }
}
*/
object deleteHbaseData2 extends Serializable{


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("deleteHbaseData2").getOrCreate()
    val sc = spark.sparkContext
    val userIDrdd = sc.parallelize(Seq("20cb992f-0947-4caa-961a-df9a60e60551",
      "21feda84-f674-4368-a93c-ce27737895e5", "2d2b6097-09f4-495b-a1b8-1ab2e1414472",
      "5388f26d-d98a-4d57-981f-b0b641e99673", "66652c47-e75a-4f82-886b-daa320c54a1a",
      "6b18aacb-b2b8-4618-8abf-fec7e85005f1", "91bd68b9-2cb0-4f2f-9ee1-cc2fa473829c",
      "d5e13d39-4889-4c26-95ec-1d5fb4d8d495")).collect().toList


    val conf = HBaseConfiguration.create()
    //指定输出格式和输出表名
    val outputTable = "t_hbaseSink"
    @transient val table = new HTable(conf, outputTable)


    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, outputTable) //设置输入表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    //提取hbase数据，并对数据进行过滤
    val rowkeyList = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      (rowkey, userID)
    }
    }.filter(x => null != x._1 & null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        val userID = Bytes.toString(x._2)
        (rowkey, userID)
      }
      }.filter(x => {userIDrdd.contains(x._2)}).map(_._1).collect()


    val connection = HConnectionManager.createConnection(conf)
    val table2 = connection.getTable(outputTable)
    
    val deleteList: util.ArrayList[Delete] = new util.ArrayList[Delete]

    for (r <- rowkeyList) {
      val key = Bytes.toBytes(r)
      val delete = new Delete(key)
      deleteList.add(delete)
      //deleteList.add(new Delete(Bytes.toBytes(r)))
    }

    table2.delete(deleteList)
    connection.close()

    table2.close()


  }
}
