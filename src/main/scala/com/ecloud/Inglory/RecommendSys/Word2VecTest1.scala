package com.ecloud.Inglory.RecommendSys

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/2/27.
 * Word2Vec分析（RDD版）
 * 读取hbase表yeeso-test-ywk_webpage中的内容列，分词、停用词过滤。
 * 构建Word2Vec模型，进行同义词查找
 * 测试成功！
 */
object Word2VecTest1 {

  def main(args: Array[String]) {

    SetLogger //不显示日志

    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("Word2VecTest1").getOrCreate()
    val sc = spark.sparkContext
    //load stopwords file
    val stopWordsPath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopWordsPath).collect().toList

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage


    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbae数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列
      (content)
    }
    }.filter(x => null != x).
      map { x => {
        val content_1 = Bytes.toString(x)
        (content_1)
      }
      }.filter(x => x.length > 0)
    //对数据进行分词
    val words = hbaseRDD.map { x =>
      val corpus = ToAnalysis.parse(x).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
      (corpus) //分词
    }
    //build word2vec model
    val word2Vec = new Word2Vec()
    val model = word2Vec.fit(words)
    //find synonyms
    val synonyms = model.findSynonyms("科技", 10)
    for ((synonym, consinSimilarity) <- synonyms) {
      println(s"$synonym $consinSimilarity")
    }
    println("========使用Array测试=========")
    val keyWords = Vector("科技", "创新")
    val synonyms2 = sc.parallelize(keyWords).map { x =>
      (x, try {
        model.findSynonyms(x, 10).map(_._1).mkString(";")
      } catch {
        case e: Exception => ""
      })
    }

    val temp = synonyms2.map(x => {
      val x2 = x._2.split(";")
      (x._1, x2.toIterable)
    }).flatMap { x =>
      val y = x._2
      for (w <- y) yield (x._1, w)
    }.filter(x => x._2 != "").filter(x => x._2.length >= 2)

    //    println(temp.foreach(println))
    //    temp.foreach(println)
    //    synonyms2.saveAsTextFile("hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/tempTest")
    //    temp.saveAsTextFile("hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/tempTest")

    println("========使用Array测试=========over")


    //防止路径下该文件夹存在
    val modelPath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Word2VecModel"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(modelPath), true)
    } catch {
      case _: Throwable => {}
    }

    model.save(sc, modelPath)
    val word2vecModel = Word2VecModel.load(sc, modelPath)

    sc.stop()

  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
}
