package com.ecloud.Inglory.DocsSimilarity

import com.ecloud.Inglory.Solr.AddIndex
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, ResultScanner, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.spark.ml.feature.{HashingTF, IDF, MinHashLSH}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/11/6.
 *
 * 计算相似性文章数小于3的文章的相似文章。
 * 计算方法，采用DocsimiTitleV5的方法
 *
 * 测试代码
 *
spark-submit \
--class com.ecloud.Inglory.DocsSimilarity.DocsimiSupplyV1 \
--master yarn \
--deploy-mode client \
--num-executors 6 \
--executor-cores 4 \
--executor-memory 8g \
--conf spark.default.parallelism=200 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
/root/lulu/Progect/ylzx/RecommendSysV1-1.0-SNAPSHOT-jar-with-dependencies.jar \
yilan-total-analysis_webpage ylzx_xgwz ylzx_xgwz_temp 1
 *
 * 运行时间：
 *
 */
object DocsimiSupplyV1 {
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class DocsimiSchema(id: String, simID: String, simScore: Double)

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
      }).filter(_.simScore >= 0.2)
    hbaseRDD
  }
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

    val docSimiRdd = getDocsimiData(docSimiTable, sc)
    val docSimiDS = spark.createDataset(docSimiRdd)
    // 获取相关文章表中，相似性文章数量大于3的文章ID
    val simW = Window.partitionBy("id").orderBy(col("simScore").desc)
    val docSimiFilter = docSimiDS.withColumn("rn", row_number.over(simW)).
      where(col("rn") > 3).
      select("id").dropDuplicates().withColumnRenamed("id", "itemString")


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
    val mhTransformedSupply = mhTransformed.join(docSimiFilter, Seq("itemString"), "leftanti")
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformedSupply, mhTransformed, 0.85) //1.0

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
