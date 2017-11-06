package com.ecloud.Inglory.DocsSimilarity

import com.ecloud.Inglory.RecommendModel.contentModelV5.DocsimiSchema
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/6.
 *
 * 计算相似性文章数小于3的文章的相似文章。
 * 计算方法，采用DocsimiTitleV5的方法
 */
object DocsimiSupplyV1 {
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  case class DocsimiSchema(id: String, simsID: String, simsScore: Double)

  def getDocsimiData(tableName: String, sc: SparkContext): RDD[DocsimiSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
      val simsID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
      val simsScore = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore

      (id, simsID, simsScore)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3).
      map(x => {
        val id = Bytes.toString(x._1)
        val simsID = Bytes.toString(x._2)
        val simsScore = Bytes.toString(x._3).toDouble
        val simsScore2 = (-1 * simsScore) + 1.0

        DocsimiSchema(id, simsID, simsScore2)
      }).filter(_.simsScore >= 0.2)
    hbaseRDD
  }


  def main(args: Array[String]) {

    // 不输出日志

    DocsimiUtil.SetLogger

    /*
1. bulid spark environment
*/

    val sparkConf = new SparkConf().setAppName(s"ylzx_xgwz_DocsimiSupplyV1") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val docSimiTable = args(1)
    val tempDocSimiTable = args(2)

    /*

    val ylzxTable = "yilan-total-analysis_webpage"
    val docSimiTable = "ylzx_xgwz"
    val tempDocSimiTable = "ylzx_xgwz_temp"
     */

    // 加载词典
    val userDefineFile = "/personal/sunlu/ylzx/userDefine.dic"
    val userDefineList = sc.broadcast(sc.textFile(userDefineFile).collect().toList)

    /*
   3. get data
    */
    val year = args(3).toInt
    val ylzxRDD = DocsimiUtil.getYlzxYRDD2(ylzxTable, year, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).//dropDuplicates(Array("title", "time", "columnId")).
      drop("columnId")

    /*
 4. 分词
  */
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(title: String, content: String): Seq[String] = {
      //加载词典
      userDefineList.value.foreach(x => {
        UserDefineLibrary.insertWord(x, "userDefine", 1000)
      })

      //每篇文章提取5个关键词
      val kwc = new KeyWordComputer(5)
      val keywords = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.mkString(" ")
      val combinedWords = title + keywords
      val seg = ToAnalysis.parse(combinedWords).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }

    val segWordsUDF = udf((title: String, content: String) => segWordsFunc(title, content))
    val segDF = ylzxDS.withColumn("segWords", segWordsUDF($"title", $"content")).drop("content") //.filter(!$"segWords".contains("null"))



    sc.stop()
    spark.stop()
  }

}
