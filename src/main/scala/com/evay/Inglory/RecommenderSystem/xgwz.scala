package com.evay.Inglory.RecommenderSystem

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
 * Created by sunlu on 17/4/6.
 * 一览资讯：相关文章
 */
object xgwz {
  def main(args: Array[String]) {

    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("xaingguanwenzhang").getOrCreate()
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
      }.filter(x => x._2.length > 1 & x._3.length > 20).filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time =  dateFormat.format(date)
        (x._1, x._2, x._3, x._4, time, x._6)
      })

    //对hbase数据进行分词
    val wordsUrlLab = hbaseRDD.map { x =>
      val corpus = ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
      (corpus, x._1, x._2, x._4, x._5, x._6) //分词、urlID、标题、label、time、websitename
    }.zipWithUniqueId().map(x =>{
      val corpus = x._1._1
      val id = x._2//Long类型
      val urlID = x._1._2
      val title = x._1._3
      val label = x._1._4
      val time = x._1._5
      val webName = x._1._6
      (corpus, id, urlID, title, label, time, webName)//分词、id、urlID、标题、label、time、websitename
    })

    // make schema1
    val schema1 = StructType(
      StructField("doc2", StringType) ::
        StructField("url2id", StringType) ::
        StructField("title2", StringType) ::
        StructField("label2", StringType) ::
        StructField("time2", StringType) ::
        StructField("websitename2", StringType)
        :: Nil)
    val schema2 = StructType(
      StructField("doc1", StringType) ::
        StructField("urlID", StringType)
        :: Nil)
    //RDD to RowRDD
    val wordsUrlLabRowRDD = wordsUrlLab.map { x => Row(x._2.toString, x._3, x._4, x._5, x._6, x._7) }//doc2,url2id,title2,label2,time2,websitename2

    val wordsUrlLabRowRDD2 = wordsUrlLab.map { x => Row(x._2.toString, x._3) }//doc1, urlID

    //RowRDD to DF
    val wordsUrlLabDF = spark.createDataFrame(wordsUrlLabRowRDD, schema1)
    val wordsUrlLabDF2 = spark.createDataFrame(wordsUrlLabRowRDD2, schema2)

    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量

    val tf_num_pairs = wordsUrlLab.map { x => {
      val tf = hashingTF.transform(x._1)
      (x._2, tf)
    }
    }
    tf_num_pairs.persist()

    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.values)

    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))

    val tfidf = num_idf_pairs.map { case (i, v) => new IndexedRow(i, v) }

    val indexed_matrix = new IndexedRowMatrix(tfidf)

    val transposed_matrix = indexed_matrix.toCoordinateMatrix.transpose()

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.1.toDouble
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
    val docSimsRowRDD = exact.entries.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      Row(doc1, doc2, sims2)
    }
    }
    val docSimsDF = spark.createDataFrame(docSimsRowRDD, schema3)

    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val dfTop5 = docSimsDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val joinDF = dfTop5.join(wordsUrlLabDF, Seq("doc2"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2
    val joinDF2 = joinDF.join(wordsUrlLabDF2, Seq("doc1"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2,urlID


    joinDF2.rdd.map(row => (row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9))).
      map { x => {
        ////sims, rn, url2id, title2, label2, time2, websitename2, urlID
        val paste = x._8 + "::score=" + x._2.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,sims
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._2.toString))//rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._3.toString))//url2id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._4.toString))//title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._5.toString))//label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._6.toString))//time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString))//websitename2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._8.toString))//urlID
        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }

}
