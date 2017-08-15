package com.ecloud.Inglory.word2Vec


import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by sunlu on 17/8/15.
 * 使用ml.feature.Word2Vec方法构建Word2Vec Model，并将模型保存到HDFS中
 */
object BuildWord2VecModelDF {

  case class YlzxSchema(content: String)


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


  def getYlzxRDD(ylzxTable:String, sc:SparkContext):RDD[YlzxSchema] = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content列
      (urlID, content)
    }
    }.filter(x => null != x._2).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val content_1 = Bytes.toString(x._2).replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
        YlzxSchema(content_1)
      }
      }.filter(x => {x.content.length >= 20})

    hbaseRDD

  }


  def main(args: Array[String]): Unit = {

//    SetLogger

    // build spark environment
    val conf = new SparkConf().setAppName(s"BuildWord2VecModelDF")//.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val ylzxRDD = getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content")
    //load stopwords file
    val stopwordsFile = "/personal/sunlu/Project/docsSimi/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //    val keyWordsDic = sc.textFile("词典所在路径").collect().toList

    //在用词典未加载前可以通过,代码方式方式来加载
    MyStaticValue.userLibrary = "library/userDefine.dic"
//    MyStaticValue.userLibrary = "/personal/sunlu/Project/docsSimi/userDefine.dic"


    //定义UDF
    //分词、词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
    })

    val segDF = ylzxDF.withColumn("segWords", segWorsd(column("content")))

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVectorSize(10) // 1000
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(segDF)
    word2VecModel.write.overwrite().save("/personal/sunlu/Project/docsSimi/Word2VecModelDF")


    sc.stop()
    spark.stop()
  }
}
