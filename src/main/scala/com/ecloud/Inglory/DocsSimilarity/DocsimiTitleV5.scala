package com.ecloud.Inglory.DocsSimilarity

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ecloud.Inglory.Solr.AddIndex
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, ResultScanner}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, MinHashLSH}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Created by sunlu on 17/10/17.
 * 使用DocsimiTitleV4的方法，将天数和年设为变量
 *
spark-submit \
--class com.ecloud.Inglory.DocsSimilarity.DocsimiTitleV5 \
--master yarn \
--deploy-mode client \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 8g \
--conf spark.default.parallelism=200 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
/root/lulu/Progect/ylzx/RecommendSysV1-1.0-SNAPSHOT-jar-with-dependencies.jar \
yilan-total-analysis_webpage ylzx_xgwz ylzx_xgwz_temp 100 1

 运行时间：31mins, 47sec
 *
 */
object DocsimiTitleV5 {

  def DeleteAndSaveTable(tableName: String, resultDF: DataFrame): Unit = {
    val hbaseConf = HBaseConfiguration.create()

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(tableName)) {
      hAdmin.disableTable(tableName)
      hAdmin.deleteTable(tableName)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(tableName))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //设置输出表名

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

    println("delete and save " + tableName + " succeed!")
  }

  def SaveTable(tableName: String, resultDF: DataFrame): Unit = {
    val hbaseConf = HBaseConfiguration.create()

    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表 => START
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(tableName)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
      //      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    } else {
      print("Table  Exists!  not Create Table")
    }
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //设置输出表名

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

    println("save " + tableName + " succeed!")
  }

  def main(args: Array[String]) {

    // 不输出日志

    DocsimiUtil.SetLogger

    /*
1. bulid spark environment
*/

    val sparkConf = new SparkConf().setAppName(s"ylzx_xgwz_DocsimiTitleV5") //.setMaster("local[*]").set("spark.executor.memory", "2g")
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

    MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171024.txt"// bigdata7路径

    /*
2. 获取7天前的时间
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
    val N = args(3).toInt
    cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val oneMonthAgo = dateFormat.format(cal.getTime())
    val oneMonthAgoL = dateFormat.parse(oneMonthAgo).getTime

    /*
    3. get data
     */
    val year = args(4).toInt
    val ylzxRDD = DocsimiUtil.getYlzxYRDD2(ylzxTable, year, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).//dropDuplicates(Array("title", "time", "columnId")).
      drop("columnId")

    //    ylzxDS.filter($"title".contains("浙江瓯海国税")).select("itemString","title").show(false)
    //    ylzxDS.filter($"itemString" ==="d582512f-1b36-4b5c-b7df-4c1ddde93b6c").select("itemString","title").show(false)
    // get 'yilan-total-analysis_webpage','d582512f-1b36-4b5c-b7df-4c1ddde93b6c'

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

    val colRenamed = Seq("doc1", "doc2", "doc2_title", "doc2_label", "doc2_websitename", "doc2_time", "distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.itemString", "datasetB.itemString", "datasetB.title",
      "datasetB.manuallabel", "datasetB.websitename", "datasetB.time", "distCol").toDF(colRenamed: _*).
      filter($"doc1" =!= $"doc2").filter($"distCol" >= 0.01)

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("doc1").orderBy(col("distCol").asc)
    val mhSortedDF = mhSimiDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val resultDF = mhSortedDF.select("doc1", "doc2", "distCol", "rn", "doc2_title",
      "doc2_label", "doc2_time", "doc2_websitename")

    resultDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //    resultDF.filter($"doc1" === "d582512f-1b36-4b5c-b7df-4c1ddde93b6c").show(false)
    DeleteAndSaveTable(tempDocSimiTable, resultDF)
    // 对相关文章构建索引
    val solrUrl_similarArticleRec: String = "http://192.168.37.11:8983/solr/solr-yilan-similarArticleRec"
    //    val tableName_similarArticle: String = "ylzx_xgwz"
    val results_similarArticle: ResultScanner = AddIndex.getAllRows(tempDocSimiTable)
    //    AddIndex.purgAllIndex(solrUrl_similarArticleRec)//删除所有索引
    AddIndex.addIndex_SimilarArticle(results_similarArticle, solrUrl_similarArticleRec)

    SaveTable(docSimiTable, resultDF)
    resultDF.unpersist()

    sc.stop()
    spark.stop()

  }

}
