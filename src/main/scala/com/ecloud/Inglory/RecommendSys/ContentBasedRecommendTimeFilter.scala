package com.ecloud.Inglory.RecommendSys

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, IndexedRowMatrix, IndexedRow}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/2/16.
 * 对数据进行过滤，过滤近一年的数据。然后使用过滤后的数据计算文本之间的相似性。
 * 采用分布式矩阵计算文本之间的相似性
 * IndexedRow => IndexedRowMatrix => toCoordinateMatrix => transpose => toRowMatrix => columnSimilarities => .entries
 * 计算yeeso_webpage表中文档之间的相似性。
 */
object ContentBasedRecommendTimeFilter {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //定义时间格式
    val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 1
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    // cal.add(Calendar.YEAR, -N)//获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val sparkConf = new SparkConf().setAppName("ContentBasedRecommendTimeFilter")
    val sc = new SparkContext(sparkConf)
    //    val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    //load stopwords file
    val hdfs = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(hdfs).collect().toList

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数t_yeeso100
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
//限制所要扫描表的列
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c"))
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("lab"))
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"))
    scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified"))

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas")) //URL列
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("lab")) //标签列
      val time = v.getValue(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified")) //时间列
      (key, title, content, url, label, time)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val url_1 = Bytes.toString(x._4)
        val label_1 = Bytes.toString(x._5)
        val time_1 = Bytes.toString(x._6)
        val time_L = dateFormat.parse(time_1).getTime
        (rowkey_1, title_1, content_1, url_1, label_1, time_1, time_L)
      }
      }.filter(x => x._3.length > 0 & x._4.length > 0 & x._5.length > 0 & x._6.length > 0).
      filter(x => x._7 >= nDaysAgoL)

    val wordsUrlLab = hbaseRDD.map { x =>
      val corpus = ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
      (corpus, x._1, x._2, x._4, x._5, x._6) //分词、rowkey、标题、url、label、time
    }.zipWithUniqueId().map(x =>{
      val corpus = x._1._1
      val id = x._2//Long类型
      val rowkey = x._1._2
      val title = x._1._3
      val url = x._1._4
      val label = x._1._5
      val time = x._1._6
      (corpus, id, rowkey, title, url, label, time)//分词、id、rowkey、标题、url、label、time
    })


    import sqlContext.implicits._
    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)//用此行会报错，使用val sqlContext = new SQLContext(sc)运行正常
    // make schema2
    val schema1 = StructType(
      StructField("doc2", StringType) ::
        StructField("tittle", StringType) ::
        StructField("url2", StringType) ::
        StructField("label", StringType) ::
        StructField("time", StringType)
        :: Nil)
    val schema2 = StructType(
      StructField("doc1", StringType) ::
        StructField("rowkey", StringType) ::
        StructField("url1", StringType)
        :: Nil)
    //RDD to RowRDD
    val wordsUrlLabRowRDD = wordsUrlLab.map { x => Row(x._2.toString, x._4, x._5, x._6, x._7) }

    val wordsUrlLabRowRDD2 = wordsUrlLab.map { x => Row(x._2.toString, x._3, x._5) }

    //RowRDD to DF
    val wordsUrlLabDF = sqlContext.createDataFrame(wordsUrlLabRowRDD, schema1)
    val wordsUrlLabDF2 = sqlContext.createDataFrame(wordsUrlLabRowRDD2, schema2)


    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)

    val tf_num_pairs = wordsUrlLab.map { x => {
      val tf = hashingTF.transform(x._1)
      (x._2, tf)
    }
    }
    tf_num_pairs.cache()

    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.values)

    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))

    val tfidf = num_idf_pairs.map { case (i, v) => new IndexedRow(i, v) }

    val indexed_matrix = new IndexedRowMatrix(tfidf)

    val transposed_matrix = indexed_matrix.toCoordinateMatrix.transpose()

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5
    val upper = 1.0

    val sim = transposed_matrix.toRowMatrix.columnSimilarities(threshhold)
    val exact = transposed_matrix.toRowMatrix.columnSimilarities()

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }

    // make schema3
    val schema3 = StructType(
      StructField("doc1", StringType) ::
        StructField("doc2", StringType) ::
        StructField("sims", DoubleType)
        :: Nil)
    val docSimsRowRDD = sim.entries.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      Row(doc1, doc2, sims2)
    }
    }
    val docSimsDF = sqlContext.createDataFrame(docSimsRowRDD, schema3)

    //对dataframe进行分组排序，并取每组的前10个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy($"doc1").orderBy($"sims".desc)
    val dfTop2 = docSimsDF.withColumn("rn", row_number.over(w)).where($"rn" <= 10)

    val levelDF = dfTop2.withColumn("level", dfTop2("rn") * (-0.1) + 1).drop("rn") //0.9~0
    //    val levelDF = dfTop2.withColumn("level", (dfTop2("rn") -1) * (-0.1) + 1).drop("rn") //1~0.1
    val joinDF = levelDF.join(wordsUrlLabDF, levelDF("doc2") === wordsUrlLabDF("doc2"))
    //doc1,doc2,sims,level,doc2,tittle,url2,label,time
    val joinDF2 = joinDF.join(wordsUrlLabDF2, joinDF("doc1") === wordsUrlLabDF2("doc1"))
    //doc1,doc2,sims,level,doc2,tittle,url2,label,time,doc1,rowkey,url1


    joinDF2.rdd.map(row => (row(2), row(3), row(5), row(6), row(7), row(8), row(10), row(11))).
      map { x => {
        val paste = x._7 + "::score=" + x._1.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(x._8.toString)) //标签的family:qualify
        put.add(Bytes.toBytes("info"), Bytes.toBytes("relateUrl"), Bytes.toBytes(x._4.toString))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._1.toString))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._2.toString))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("tittle"), Bytes.toBytes(x._3.toString))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("label"), Bytes.toBytes(x._5.toString))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(x._6.toString))

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }
}
