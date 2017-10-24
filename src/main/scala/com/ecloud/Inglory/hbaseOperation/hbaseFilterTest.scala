package com.ecloud.Inglory.hbaseOperation

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.{TableName, HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/3/31.
 */
object hbaseFilterTest {
  def main(args: Array[String]) {

    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("hbaseFilterTest").getOrCreate()
    val sc = spark.sparkContext
    //load stopwords file
    val hdfs = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(hdfs).collect().toList

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")//yyyy-MM-dd HH:mm:ss
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 20
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N)//获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "ztb-test-yangch-2_webpage")


    val outputTable = args(1)
    /*
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(outputTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(outputTable)
      tableDesc.addFamily(new HColumnDescriptor("p".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  Remove Table and then Create Table")
      hadmin.disableTable(outputTable)
      hadmin.deleteTable(outputTable)
      val tableDesc = new HTableDescriptor(outputTable)
      tableDesc.addFamily(new HColumnDescriptor("p".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }
    */
//判断HBAE表是否存在，如果存在则删除表，然后新建表
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("p".getBytes()))
    htd.addFamily(new HColumnDescriptor("f".getBytes()))
    /*
    val hcd = new HColumnDescriptor("id")
    //add  column to table
    htd.addFamily(hcd)
    */
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"))//label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod"))//time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename"))//
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("appc"))

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名列
      val appc = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("appc")) //appc
      (urlID, title, content, label, time, webName, appc)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val label_1 = if(null != x._4)  Bytes.toString(x._4) else ""
        //时间格式转化
        val time_1 = Bytes.toLong(x._5)
        /*
                val time_1 = if(null != x._5){
                  val time = Bytes.toLong(x._5)
                  val date: Date = new Date(time)
                  val temp = dateFormat.format(date)
                  temp } else ""
         */

        val websitename_1 = if(null != x._6) Bytes.toString(x._6) else ""

        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.filter(x => x._2.length > 1 & x._3.length > 20).filter(x => x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time =  dateFormat.format(date)
        (x._1, x._2, x._3, x._4, time, x._6)//rowkey, title, content, label,time,webname
      })

     hbaseRDD.map { x => {
        val key = Bytes.toBytes(x._1)
        val put = new Put(key)
        put.add(Bytes.toBytes("p"), Bytes.toBytes("t"), Bytes.toBytes(x._2.toString))//p:t
        put.add(Bytes.toBytes("p"), Bytes.toBytes("c"), Bytes.toBytes(x._3.toString))//p:c
        put.add(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._4.toString))//p:manuallabel
        put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Bytes.toBytes(x._5.toString))//f:mod
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitename"), Bytes.toBytes(x._6.toString))//p:websitename

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    sc.stop()
  }
}
