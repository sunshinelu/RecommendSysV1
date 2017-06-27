package com.ecloud.Inglory.LDA

import java.text.SimpleDateFormat
import java.util.Date

import com.ecloud.Inglory.RecommendSys.alsDataProcessedV4.Schema
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/6.
 * 读取HBASE数据构建LDA模型
 */
object hbaseLDA {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {

    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("hbaseLDA").getOrCreate()
    val sc = spark.sparkContext

    //load stopwords file
    val stopwordsPath = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsPath).collect().toList

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")//yyyy-MM-dd HH:mm:ss

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")

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
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))//title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))//content


    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列

      (urlID, title, content)
    }
    }.filter(x => null != x._2 & null != x._3 ).
      map ( x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val corpus = ToAnalysis.parse(content_1).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
        (urlID_1, title_1, content_1, corpus)
      }).zipWithIndex().map(x => {
      val id = x._2
      val rowkey = x._1._1
      val title = x._1._2
      val content = x._1._3
      val corpus = x._1._4
      (id, rowkey, title, corpus)
    })

    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量

    val tf_num_pairs = hbaseRDD.map { x => {
      val tf = hashingTF.transform(x._4)
      (x._1, tf)
    }
    }
    tf_num_pairs.persist()

    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.values)

    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))

    val tfidf = num_idf_pairs.map { case (i, v) => new IndexedRow(i, v) }

    val indexed_matrix = new IndexedRowMatrix(tfidf)

    //2 建立模型，设置训练参数，训练模型
    val ldaModel = new LDA().
      setK(3).
      setDocConcentration(5).
      setTopicConcentration(5).
      setMaxIterations(20).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(num_idf_pairs)


    //3 模型输出，模型参数输出，结果输出
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    // 主题分布
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }

    // 主题分布排序
    ldaModel.describeTopics(4)
    // 文档分布
    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    distLDAModel.topicDistributions.collect




    sc.stop()
  }
}
