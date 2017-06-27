package com.ecloud.Inglory.hbaseOperation

import java.util

import com.ecloud.Inglory.Streaming.logsToHbase.LogsSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.Delete

import org.apache.hadoop.hbase.client._
/**
 * Created by sunlu on 17/4/27.
 *  根据rowkey删除HBASE中的部分数据03-29, 04－12, 04－13
 */
object deleteHbaseData {
  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("deleteHbaseData").getOrCreate()
    val sc = spark.sparkContext

    val logsRDD = sc.textFile("file:///root/lulu/Workspace/spark/yeeso/RecommendSys/logs/app-ylzx-logs/*.log").
      filter(null != _).map(_.split("\t")).
      filter(_.length == 11).map(x => {
      val LOG_ID = x(0)
      val TYPE = if(null != x(1)) x(1) else ""
      val TITLE = if(null != x(2)) x(2) else ""
      val CREATE_BY = if(null != x(3)) x(3) else ""
      val CREATE_BY_ID = if(null != x(4)) x(4) else ""
      val CREATE_TIME = if(null != x(5)) x(5) else ""
      val REMOTE_ADDR = if(null != x(6)) x(6) else ""
      val USER_AGENT = if(null != x(7)) x(7) else ""
      val REQUEST_URI = if(null != x(8)) x(8) else ""
      val METHOD = if(null != x(9)) x(9) else ""
      val PARAMS = if(null != x(10)) x(10) else ""
      LogsSchema(LOG_ID, TYPE, TITLE, CREATE_BY, CREATE_BY_ID, CREATE_TIME, REMOTE_ADDR, USER_AGENT, REQUEST_URI, METHOD, PARAMS)
    })


    val conf = HBaseConfiguration.create()
    //指定输出格式和输出表名
    val outputTable = "t_hbaseSink"
    @transient val table = new HTable(conf, outputTable)

    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)


    val connection = HConnectionManager.createConnection(conf)
    val table2 = connection.getTable(outputTable)

    val rowkeyList = logsRDD.map(_.LOG_ID).collect()

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
   // val newConfig = new Configuration(conf)

/*
    val configuration = HBaseConfiguration.create();
    val connection = CoprocessorHConnection.createConnection(configuration);
    //建立表的连接
    Table table = connection.getTable(TableName.valueOf("testtable"));
    List<Delete> deletes = new ArrayList<Delete>();
    Delete delete1 = new Delete(Bytes.toBytes("row1"));
    delete1.setTimestamp(4);
    deletes.add(delete1);
    Delete delete2 = new Delete(Bytes.toBytes("row2"));
    delete2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), 5);
    deletes.add(delete2);
    Delete delete3 = new Delete(Bytes.toBytes("row3"));
    delete3.addFamily(Bytes.toBytes("colfam1"));
    delete3.addFamily(Bytes.toBytes("colfam2"), 3);
    deletes.add(delete3);
    table.delete(deletes);

*/

    /*
    val conf = HBaseConfiguration.create()
    //指定输出格式和输出表名
    val outputTable = "t_hbaseSink"
    @transient val table = new HTable(conf, outputTable)

    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)


    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    val deletes: List = new util.ArrayList()

    logsRDD.foreach(x => {
      val key = Bytes.toBytes(x.LOG_ID)
      val delete = new Delete(key)
      delete.deleteFamily(Bytes.toBytes("info"))
      table.delete(delete)
    })


    table.close()
    */

  }

}
