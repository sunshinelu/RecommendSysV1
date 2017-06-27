package com.ecloud.Inglory.hbaseOperation

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Delete, HConnectionManager, HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/22.
 * 读取yilan-total_webpage中rowkey中有空格的数据，然后删除。
 * 由于读取出的数据太多，collect和toList的时候内存溢出，所以使用sample和takeSample这两个方法进行数据抽取，然后删除。
 * Sample：对RDD中的集合内元素进行采样，第一个参数withReplacement是true表示有放回取样，false表示无放回。第二个参数表示比例，第三个参数是随机种子。如data.sample(true, 0.3,new Random().nextInt())。
 * takeSample：和sample用法相同，只不第二个参数换成了个数。返回也不是RDD，而是collect。
 * 参考链接：http://www.bubuko.com/infodetail-954616.html
 */
object deleteHbaseData3 {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("deleteHbaseData3").getOrCreate()
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()
    //指定输出格式和输出表名
    val ylzxTable = "yilan-total_webpage"
    @transient val table = new HTable(conf, ylzxTable)


    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, ylzxTable)
    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, time)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toLong(x._2)
        val time2 = new String(x._2, "UTF-8")
        (rowkey, time2)
      }
      }.filter(_._1.contains(" ")).map(_._1)

//    val sample1 = hbaseRDD.sample(false, 0.1, 12)

//    val sample1List = sample1.collect().toList

    val rowkeyList = hbaseRDD.collect().toList

    val sample2 = hbaseRDD.takeSample(false, 50000, 12).toList

    val connection = HConnectionManager.createConnection(conf)
    val table2 = connection.getTable(ylzxTable)

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






    //val rowkeyList = hbaseRDD.collect()
//    hbaseRDD.saveAsTextFile("/personal/sunlu/rowkeyList")

//    val rowkeyRDD = sc.textFile("/personal/sunlu/rowkeyList/part-00000").filter(null != _)




  }
}
