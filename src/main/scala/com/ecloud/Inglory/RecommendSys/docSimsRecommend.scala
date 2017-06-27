package com.ecloud.Inglory.RecommendSys

import com.ecloud.Inglory.RecommendSys.alsDataProcessedV4.{Schema1, LogView}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/4/7.
 * 读取docSims中计算出的文本相似性表。
 * 根据用户的浏览记录选出与用户浏览的文章相似但该用户没有浏览过的文章作为推荐出的文章，选出打分最高的前200条；
 */
object docSimsRecommend {


  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("docSimsRecommend").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //如果outputTable表存在，则删除表；如果不存在则新建表。
    val outputTable = args(1)
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(outputTable)
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"))//id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID"))//simsID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsScore"))//simsScore
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"))//level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("t"))//t
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"))//manuallabel
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod"))//mod
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("websitename"))//manuallabel

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //id列
      val simsID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID列
      val simsScore = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore列
      val level = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //level列
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("t")) //t列
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
      val time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod列
      val webName = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename
      (id, simsID, simsScore, level, title, manuallabel, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val id = Bytes.toString(x._1)
        val simsID = Bytes.toString(x._2)
        val simsScore = Bytes.toString(x._3)
        val level = Bytes.toString(x._4)
        val title = Bytes.toString(x._5)
        val manuallabel = Bytes.toString(x._6)
        val time = Bytes.toString(x._7)
        val webName = Bytes.toString(x._8)
        (id, simsID, simsScore, level, title, manuallabel, time, webName)
      }
      }.filter(x => x._5.length >= 2 & x._6.length >= 2)

    val corrRDD = hbaseRDD.map(x => {
      ((x._2, x._5, x._6, x._7, x._8),1)
      /*
      val docID = x._2
      val title = x._5
      val manuallabel = x._6
      val time = x._7
      val webName = x._8
      (docID, title, manuallabel, time, webName)
      */
    }).reduceByKey(_ + _).map(_._1).map(x => {
      val docID = x._1.toString
      val title = x._2.toString
      val manuallabel = x._3.toString
      val time = x._4.toString
      val webName = x._5.toString
      DocIdInfo(docID, title, manuallabel, time, webName)
    })
      //.map(x => {
      //(x.docID, (x.title, x.manuallabel, x.time, x.webName))
    //})
import spark.implicits._
    val corrDF = spark.createDataset(corrRDD)
    //hbaseRDD.unpersist()
    //corrRDD.persist()

    val simiRDD = hbaseRDD.map(x => {
      val similar = 1 / x._4.toDouble
      val similar2 = f"$similar%1.5f".toDouble
      DocSimi(x._1, x._2, similar2)
    })
    //simiRDD.persist()

    //read log files
    val logsRDD = sc.textFile("/app-ylzx-logs").filter(null != _)

    val logsRDD2 = logsRDD.map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView(line(4), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val logsRDD3 = logsRDD2.filter(x => x.REQUEST_URI.contains("search/getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        (userID, urlString)
      }).filter(_._1.length >= 10).filter(_._1 != null).map(x=>((x._1,x._2), 1)).reduceByKey(_ + _).map(x => {
      val useriID = x._1._1
      val docID = x._1._2
      val value = x._2.toDouble
      UserProf(useriID, docID, value)

    })


    val recommRDD = Recommend(simiRDD, logsRDD3, 200)
    //simiRDD.unpersist()

    val recommDS = spark.createDataset(recommRDD)

    val recommDS2 = recommDS.join(corrDF, Seq("docID") ,"left")


    val w = Window.partitionBy("userID").orderBy(col("score").desc)
    val ds1= recommDS2.withColumn("rn", row_number.over(w)).where(col("rn") <= 200).na.drop()
    //userID, docID, score, title, manuallabel, time, webName, rn
    ds1.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).map(x => {
      val paste = x._1 + "::score=" + x._8
      val key = Bytes.toBytes(paste)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //docID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(x._2.toString))//userID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(x._3.toString))//score
      put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(x._4.toString))//title
      put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._5.toString))//manuallabel
      put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._6.toString))//time
      put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString))//webName
      put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._8.toString))//rn
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)

    /*
    val recommRDD2 = recommRDD.map(f => (f.docID, (f.userID, f.score))).
      leftOuterJoin(corrRDD).filter(f => ! f._2._2.isEmpty).map(f => {
      val docID = f._1
      val userID = f._2._1._1
      val score = f._2._1._2
      val f2 = f._2._2.toList
      val title = f2(0).toString()
      val manuallabel = f2(1).toString()
      val time = f2(2).toString()
      val webName = f2(3).toString()

      (docID, userID, score, title, manuallabel, time, webName)
    }).zipWithIndex().map { x => {
      //docID, userID, score, title, manuallabel, time, webName
      val paste = x._1._1 + "::score=" + x._2.toString
      val key = Bytes.toBytes(paste)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //docID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(x._1._2.toString))//userID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(x._1._3.toString))//score
      put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._1._4.toString))//title
      put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._1._5.toString))//manuallabel
      put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._1._6.toString))//time
     // put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString))//webName

      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)
*/

    sc.stop()
  }

  case class DocSimi(
                       val docID: String,
                       val simiDocID: String,
                       val similar: Double) extends Serializable

  case class UserProf(
                       val userID: String,
                       val docID: String,
                       val value: Double) extends Serializable
  case class UserRecomm(
                         val userID: String,
                         val docID: String,
                         val score: Double) extends Serializable

  case class DocIdInfo(val docID: String,
                    val title: String,
                    val manuallabel: String,
                    val time: String,
                    val webName: String) extends Serializable


  def Recommend(docss_similar: RDD[DocSimi],
                user_prefer: RDD[UserProf],
                r_number: Int): (RDD[UserRecomm]) = {
    //   0 数据准备
    val rdd_app1_R1 = docss_similar.map(f => (f.docID, f.simiDocID, f.similar))
    val user_prefer1 = user_prefer.map(f => (f.userID, f.docID, f.value))
    //   1 矩阵计算——i行与j列join
    val rdd_app1_R2 = rdd_app1_R1.map(f => (f._1, (f._2, f._3))).
      join(user_prefer1.map(f => (f._2, (f._1, f._3))))
    //   2 矩阵计算——i行与j列元素相乘
    val rdd_app1_R3 = rdd_app1_R2.map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2))
    //   3 矩阵计算——用户：元素累加求和
    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x, y) => x + y)
    //   4 矩阵计算——用户：对结果过滤已有I2
    val rdd_app1_R5 = rdd_app1_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).
      filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))
    //   5 矩阵计算——用户：用户对结果排序，过滤
    val rdd_app1_R6 = rdd_app1_R5.groupByKey()
    val rdd_app1_R7 = rdd_app1_R6.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    })
    val rdd_app1_R8 = rdd_app1_R7.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    })
    rdd_app1_R8.map(f => UserRecomm(f._1, f._2, f._3))
  }


}
