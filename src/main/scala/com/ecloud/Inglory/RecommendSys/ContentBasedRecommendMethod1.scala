package com.ecloud.Inglory.RecommendSys


import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/2/8.
 * 1、提取yeeso测试数据t_yeeso100中的数据，p:t, p:c, f:bas(url列), p:lab（标签列）, h:Last-Modified
 * 2、对提取的数据进行非空过滤，并对p:c列的数据进行分词
 * 3、分词后计算tf-idf
 * 4、使用余弦相似度的方法计算文本相似性
 * 5、对结果进行数据整形，将结果保存到HBase数据库中
 */
object ContentBasedRecommendMethod1 {
  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val sparkConf = new SparkConf().setAppName("ContentBasedRecommendMethod1")
    val sc = new SparkContext(sparkConf)
    //    val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    //load stopwords file
    val hdfs = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(hdfs).collect().toList

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "t_yeeso100") //设置输入表名 第一个参数t_yeeso100/yeeso_webpage
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, "t_userProfileV2") //设置输出表名，与输入是同一个表
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val yeesoDate = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"))
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c"))
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"))
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("lab"))
      val time = v.getValue(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified"))
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
        (rowkey_1, title_1, content_1, url_1, label_1, time_1)
      }
      }.filter(x => x._3.length > 0 & x._4.length > 0 & x._5.length > 0 & x._6.length > 0)

    //对亿搜数据进行分词
    val tokens = yeesoDate.map { x =>
      ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 1).map(_ (0)).toList.
        filter(word => word.length >= 1).filter(x => !stopwords.contains(x)).toSeq
    }.zipWithIndex()
    tokens.cache()
    val wordsUrlLab = yeesoDate.map { x =>
      val corpus = ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
        filter(_.length >= 1).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
      val wordsMap = corpus.zipWithIndex.toMap
      val keysTokens = wordsMap.keys
      val ID = wordsMap.values.toString()
      (keysTokens, ID, x._1, x._2, x._4, x._5, x._6) //分词、ID、rowkey、标题、url、label、time
    }
    import org.apache.spark.sql.functions._
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
    val wordsUrlLabRowRDD = wordsUrlLab.map { x => Row(x._2, x._4, x._5, x._6, x._7) }

    val wordsUrlLabRowRDD2 = wordsUrlLab.map { x => Row(x._2, x._3, x._5) }

    //RowRDD to DF
    val wordsUrlLabDF = sqlContext.createDataFrame(wordsUrlLabRowRDD, schema1)
    val wordsUrlLabDF2  = sqlContext.createDataFrame(wordsUrlLabRowRDD2, schema2)



    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量
    val tf_num_pairs2 = tokens.map {
      case (seq, num) =>
        val tf = hashingTF.transform(seq)
        (num, tf)
    }

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

    //广播一份tf-idf向量集
    val b_num_idf_pairs = sc.broadcast(num_idf_pairs.collect())

    //计算doc之间余弦相似度
    val docSims = num_idf_pairs.flatMap {
      case (id1, idf1) =>
        val idfs = b_num_idf_pairs.value.filter(_._1 != id1)
        val sv1 = idf1.asInstanceOf[SV]
        import breeze.linalg._
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        idfs.map {
          case (id2, idf2) =>
            val sv2 = idf2.asInstanceOf[SV]
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            val cosSim = bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
            //保留cosSim中有效数字
            val cosSim2 = f"$cosSim%1.5f"
            (id1, id2, cosSim2)
        }
    }
    import sqlContext.implicits._
    //    case class docSimsDF(doc1: String, doc2: String, sim: Double)
    //将docSims转成dataframe
    // make schema3
    val schema3 = StructType(
      StructField("doc1", StringType) ::
        StructField("doc2", StringType) ::
        StructField("sims", DoubleType)
        :: Nil)

    val docSimsRowRDD = docSims.map { x => {
      val doc1 = x._1.toString
      val doc2 = x._2.toString
      val sims = x._3.toDouble
      Row(doc1, doc2, sims)
    }
    }
    val docSimsDF = sqlContext.createDataFrame(docSimsRowRDD,schema3)


    //对dataframe进行分组排序，并取每组的前10个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy($"doc1").orderBy($"sims".desc)
    val dfTop2 = docSimsDF.withColumn("rn", row_number.over(w)).where($"rn" <= 10)
    //val dfTop2 = test3.withColumn("rn", rowNumber.over(w)).where($"rn" <= 10)
    val levelDF = dfTop2.withColumn("level", dfTop2("rn") * (-0.1) + 1).drop("rn")//0.9~0
    //    val levelDF = dfTop2.withColumn("level", (dfTop2("rn") -1) * (-0.1) + 1).drop("rn") //1~0.1
    val joinDF = levelDF.join(wordsUrlLabDF, levelDF("doc2") === wordsUrlLabDF("doc2"))
    //doc1,doc2,sims,doc2,tittle,url2,label,time
    val joinDF2 = joinDF.join(wordsUrlLabDF2, joinDF("doc1") === wordsUrlLabDF2("doc1"))
    //doc1,doc2,sims,doc2,tittle,url2,label,time,doc1,rowkey,url1


    joinDF2.rdd.map(row => ( row(2), row(3), row(5), row(6), row(7), row(8), row(10), row(11))).
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

    //    val sorted = test3.sort($"sims".desc, $"doc1")//sims列降序排序，doc1列升序排序


    sc.stop()


  }
}
