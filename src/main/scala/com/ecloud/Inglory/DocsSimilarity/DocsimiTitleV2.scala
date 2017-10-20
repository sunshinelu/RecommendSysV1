package com.ecloud.Inglory.DocsSimilarity

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.app.keyword.KeyWordComputer
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinHashLSH, IDF, HashingTF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/9/29.
 * 使用文章标题和5个关键词计算文章之间的相似性
 * 计算近一月的文章和近一年文章之间的文章相似性
 * 使用算法为：Jaccard Distance
 *
spark-submit \
--class com.ecloud.Inglory.DocsSimilarity.DocsimiTitleV2 \
--master yarn \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/ylzx/RecommendSysV1.jar \
yilan-total-analysis_webpage ylzx_xgwz
 *
 */
object DocsimiTitleV2 {

  def main(args: Array[String]) {
    // 不输出日志

    DocsimiUtil.SetLogger

    /*
1. bulid spark environment
*/

    val sparkConf = new SparkConf().setAppName(s"ylzx_xgwz_DocsimiTitleV2") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val docSimiTable = args(1)


    /*

    val ylzxTable = "yilan-total-analysis_webpage"
    val docSimiTable = "ylzx_xgwz"
     */

    /*
2. 获取一个月前的时间
 */

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
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
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val oneMonthAgo = dateFormat.format(cal.getTime())
    val oneMonthAgoL = dateFormat.parse(oneMonthAgo).getTime
    /*
    3. get data
     */
    val ylzxRDD = DocsimiUtil.getYlzxYRDD(ylzxTable, 1, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)

    /*
    4. 分词
     */
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(title: String, content: String): Seq[String] = {
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

    //  case class YlzxSchema(itemString: String, title: String, manuallabel: String, time: String,timeL:Long, websitename: String, content: String)

    val segWordsUDF = udf((title: String, content: String) => segWordsFunc(title, content))
    val segDF = ylzxDS.withColumn("segWords", segWordsUDF($"title", $"content")).drop("content") //.filter(!$"segWords".contains("null"))
    /*
     5. calculate tf-idf value
       */
    val hashingTF = new HashingTF().
        setInputCol("segWords").
        setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)

    /*
6. using Jaccard Distance calculate doc-doc similarity
计算近一月的文章与近一年的文章之间的相似性
    */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(tfidfData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(tfidfData)
    val mhTransformedOneMonth = mhTransformed.filter($"timeL" >= oneMonthAgoL)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformedOneMonth, mhTransformed, 1.0)
    //  case class YlzxSchema(itemString: String, title: String, manuallabel: String, time: String,timeL:Long, websitename: String, content: String)

    val colRenamed = Seq("doc1","doc2", "doc2_title", "doc2_label", "doc2_websitename", "doc2_time", "distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.itemString", "datasetB.itemString", "datasetB.title",
      "datasetB.manuallabel", "datasetB.websitename", "datasetB.time", "distCol").toDF(colRenamed: _*).
      filter($"doc1" =!= $"doc2").filter($"distCol" >= 0.01)

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("doc1").orderBy(col("distCol").asc)
    val mhSortedDF = mhSimiDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val resultDF = mhSortedDF.select("doc1", "doc2", "distCol", "rn", "doc2_title",
      "doc2_label", "doc2_time", "doc2_websitename")

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    //    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名


    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
      //      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    } else {
      print("Table  Exists!  not Create Table")
    }

    /*
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(docSimiTable)) {
      hAdmin.disableTable(docSimiTable)
      hAdmin.deleteTable(docSimiTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(docSimiTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER
    */
    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, docSimiTable) //设置输出表名


    //    val table = new HTable(hbaseConf,docSimiTable)
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,docSimiTable)

    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    resultDF.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map { x => {
        //("doc1", "doc2", "distCol", "rn", "doc2_title", "doc2_label", "doc2_time", "doc2_websitename")
        val paste = x._1.toString + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //doc1
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._2.toString)) //doc2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._3.toString)) //value
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._5.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._8.toString)) //websitename2

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()
  }
}
