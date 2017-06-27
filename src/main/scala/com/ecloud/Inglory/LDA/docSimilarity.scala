package com.ecloud.Inglory.LDA

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ecloud.Inglory.word2Vec.docSimilarity.{docSimsSchema, HbaseSchema}
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.io.Text

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, IndexedRowMatrix, IndexedRow}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/5/22.
 * 使用LDA模型的方法计算文章相似性
 * 运行成功！
 */
object docSimilarity {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
//    SetLogger

    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("LDA: docSimilarity").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") //yyyy-MM-dd HH:mm:ss
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 20
//    cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-total_webpage") //ztb-test-yangch-2_webpage
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1


    val outputTable = args(1)
    //判断HBAE表是否存在，如果存在则删除表，然后新建表
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
   // htd.addFamily(new HColumnDescriptor("f".getBytes()))
    /*
    val hcd = new HColumnDescriptor("id")
    //add  column to table
    htd.addFamily(hcd)
    */
    hAdmin.createTable(htd)



    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //
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
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val label_1 = if (null != x._4) Bytes.toString(x._4) else ""
        //时间格式转化
        val time_1 = Bytes.toLong(x._5)

        val websitename_1 = if (null != x._6) Bytes.toString(x._6) else ""

        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.filter(x => x._2.length > 1 & x._3.length > 20).filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time = dateFormat.format(date)
        (x._1, x._2, x._3, x._4, time, x._6)
      }).map { x =>

      //每篇文章提取25个关键词
      val kwc = new KeyWordComputer(25)
      val keywords = kwc.computeArticleTfidf(x._2, x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
/*
      //使用ansj分词
      val corpus = ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
*/
      (keywords, x._1, x._2, x._4, x._5, x._6) //关键词、urlID、标题、label、time、websitename
    }.zipWithUniqueId().map(x => {
      val keywords = x._1._1
      val id = x._2 //Long类型
      val urlID = x._1._2
      val title = x._1._3
      val label = x._1._4
      val time = x._1._5
      val webName = x._1._6
      HbaseSchema(keywords, id, urlID, title, label, time, webName) //关键词、id、urlID、标题、label、time、websitename
    }).repartition(220)//.filter(_.segWords.size >= 2)

    val ds1 = spark.createDataset(hbaseRDD).dropDuplicates("segWords")

    val vocabSize: Int = 2900000

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .setMinDF(2)
      .fit(ds1)
    val ds2 = cvModel.transform(ds1).select("id","features")

    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(ds2)
    val ds3 = model.transform(ds2)

    val rdd2 = ds3.select("id","topicDistribution").rdd.map{
      case Row(id:Long, features: org.apache.spark.ml.linalg.Vector) =>  (id, Vectors.fromML(features).toSparse)
    }
    val rdd3 = rdd2.map { case (i, v) => new IndexedRow(i, v) }
    val matLDA = new IndexedRowMatrix(rdd3)
    val transposed_mat = matLDA.toCoordinateMatrix.transpose()
    val simLDA = transposed_mat.toRowMatrix.columnSimilarities(0.2)
    val simLDA_2 = simLDA.entries.filter { case MatrixEntry(i, j, u) => u >= 0.2 && u <= 1 }.repartition(220)

    val docSimsLDA = simLDA_2.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }

    val ds4 = spark.createDataset(docSimsLDA)
    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val ds5 = ds4.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val wordsUrlLabDF = ds1.withColumnRenamed("id", "doc2").withColumnRenamed("urlID", "url2id").
      select("doc2", "url2id", "title", "label", "time", "websitename")

    val wordsUrlLabDF2 = ds1.withColumnRenamed("id", "doc1").select("doc1", "urlID")

    val ds6 = ds5.join(wordsUrlLabDF, Seq("doc2"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2
    val ds7 = ds6.join(wordsUrlLabDF2, Seq("doc1"), "left").na.drop()
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2,urlID

    // doc1, doc2, sims, rn, url2id,  title, label, time, websitename, urlID

    ds7.rdd.map(row => (row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9))).
      map { x => {
        //sims, rn, url2id,  title, label, time, websitename, urlID
        val paste = x._8 + "::score=" + x._2.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,sims
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._2.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._3.toString)) //url2id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._4.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._5.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._6.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString)) //websitename2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._8.toString)) //urlID
        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)

    /*

    val document = ds3.na.drop.select("id", "features").rdd.map {
      case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))
    }

    val temp = document.map(_._2.size)


    val vocabArray = cvModel.vocabulary // vocabulary
    val actualCorpusSize = ds3.count()

/*
    println("document.count()为" + document.count())
    println("===============")
    println("actualCorpusSize: " + actualCorpusSize)

    println("---------------------")
   document.collect.foreach(println)
    ds3.na.drop.select("id", "features").show()
*/

    // Run LDA.
    val lda = new LDA()

    val k: Int = 5
    val maxIterations: Int = 10
    val docConcentration: Double = -1
    val topicConcentration: Double = -1
    val checkpointDir: Option[String] = None
    val checkpointInterval: Int = 10
    val algorithm: String = "em"

    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setCheckpointInterval(checkpointInterval)

    val ldaModel = lda.run(document)

    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val ldaMat = distLDAModel.topicDistributions

    val tfidf = ldaMat.map { case (i, v) => new IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(tfidf)
    val transposed_matrix = mat.toCoordinateMatrix.transpose()

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val sim = transposed_matrix.toRowMatrix.columnSimilarities(threshhold)
    val exact = transposed_matrix.toRowMatrix.columnSimilarities()

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }


    val docSimsRDD = sim_threshhold.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }


    val ds4 = spark.createDataset(docSimsRDD)

    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val ds5 = ds4.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val wordsUrlLabDF = ds1.withColumnRenamed("id", "doc2").withColumnRenamed("urlID", "url2id").
      select("doc2", "url2id", "title", "label", "time", "websitename")

    val wordsUrlLabDF2 = ds1.withColumnRenamed("id", "doc1").select("doc1", "urlID")

    val ds6 = ds5.join(wordsUrlLabDF, Seq("doc2"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2
    val ds7 = ds6.join(wordsUrlLabDF2, Seq("doc1"), "left").na.drop()
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2,urlID

    // doc1, doc2, sims, rn, url2id,  title, label, time, websitename, urlID

    ds7.rdd.map(row => (row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9))).
      map { x => {
        //sims, rn, url2id,  title, label, time, websitename, urlID
        val paste = x._8 + "::score=" + x._2.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,sims
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._2.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._3.toString)) //url2id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._4.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._5.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._6.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString)) //websitename2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._8.toString)) //urlID
        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)
*/
    sc.stop()
  }
}
