package com.ecloud.Inglory.LDA

import java.util.Properties
import com.ecloud.Inglory.word2Vec.docSimilarity.docSimsSchema
import org.ansj.app.keyword.KeyWordComputer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, IndexedRowMatrix, IndexedRow}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/4/6.
 */
object mysqlLDA {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  case class MysqlSchema(id: Long, rowkey: String, segWords: String, title: String, corpue: Seq[String])

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("getKeyWordsMySQL").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
//    val url1 = "jdbc:mysql://192.168.37.18:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val df1 = spark.read.jdbc(url1, "ylzx_Example", prop1).select("rowkey", "title", "content")

    val rdd1 = df1.rdd.map(row => (row(0), row(1), row(2))).map(x => {
      val id = x._1.toString
      val title = x._2.toString
      val content = x._3.toString

      val kwc = new KeyWordComputer(25)
      val result = kwc.computeArticleTfidf(title, content).toArray
      val corpus = result.map(_.toString.split("/")).filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 ).toSeq
      //将keywords拼接成字符串
      val keywords = for (ele <- result) yield ele.toString.split("/")(0)
      val keywordsString = keywords.mkString(";")

      (id, keywordsString, title, content, corpus)
    }).zipWithIndex().map(x => {
      val id = x._2
      val rowkey = x._1._1
      val keywordsS = x._1._2
      val title = x._1._3
      val content = x._1._4
      val corpus = x._1._5
      MysqlSchema(id, rowkey, keywordsS, title, corpus)
    })

    val t1 = rdd1.map(_.corpue.size).max()
    println("==============")
    println("t1 is: " + t1)
    val t2 = rdd1.map(_.corpue.size).min()
    println("==============")
    println("t2 is: " + t2)
    println("==============")
    rdd1.map(_.corpue).take(3).foreach(println)

    val ds1 = spark.createDataset(rdd1)
//    ds1.printSchema()
//    ds1.show(5)


    val vocabSize: Int = 2900000

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("corpue")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .setMinDF(2)
      .fit(ds1)
    val ds2 = cvModel.transform(ds1).select("id","features")
//    ds2.printSchema()
//    ds2.show(5)

    val vocabArray = cvModel.vocabulary // vocabulary
    val actualCorpusSize = ds2.count()
    println("==============")
    println(vocabArray.mkString(";"))
    println("==============")
    println("vocabArray size is: " + vocabArray.size)



    val k: Int = 5
    val algorithm: String = "em"
    val maxIterations: Int = 10

    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(ds2)//.asInstanceOf[DistributedLDAModel]
    val ds3 = model.transform(ds2)
    ds3.printSchema()
    ds3.show(5)
    ds3.count()

    val rdd2 = ds3.select("id","topicDistribution").rdd.map{
      case Row(id:Long, features: org.apache.spark.ml.linalg.Vector) =>  (id, Vectors.fromML(features).toSparse)
    }

    rdd2.take(5).foreach(println)
    val rdd3 = rdd2.map { case (i, v) => new IndexedRow(i, v) }
    val matLDA = new IndexedRowMatrix(rdd3)
    val transposed_mat = matLDA.toCoordinateMatrix.transpose()
    println("numCols is:" + transposed_mat.numCols())
    println("============")
    println("numRows is: " + transposed_mat.numRows())
    /*
    numCols is:1060
    numRows is: 10
     */

    val simLDA = transposed_mat.toRowMatrix.columnSimilarities(0.2)
    val simLDA_2 = simLDA.entries.filter { case MatrixEntry(i, j, u) => u >= 0.2 && u <= 1 }

    val docSimsLDA = simLDA_2.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }

    val docSimsLDAds = spark.createDataset(docSimsLDA)
    docSimsLDAds.take(5).foreach(println)

    /*
    (0,[0.0038172096662553545,0.004391421742172136,0.9666062604999827,0.0035907315723049883,0.0035798794363706547,0.0036159897693150292,0.0035887308738405004,0.0036339551039508357,0.0035890264792904844,0.0035867948565173565])
    (1,[0.003817183783745717,0.9670091868421558,0.003988981679485683,0.0035906633902649835,0.0035798137985912034,0.003615931694919289,0.0035887220153852264,0.00363378812201055,0.0035889802791812895,0.003586748394260159])
    (2,[0.4743560918244376,0.004390843235470619,0.49606727423221786,0.003590787673712743,0.0035799569710478836,0.003616088827251989,0.003588862179655748,0.0036339512838930916,0.003589262782490861,0.003586880989821618])
    (3,[0.00454639243125331,0.9607047906522505,0.004751019257705481,0.004276886624352542,0.0042639535049053805,0.004307003357669538,0.004274571963745178,0.0043282596000646005,0.004274915556770122,0.004272207051283496])
    (4,[0.438718987144599,0.5247853661254565,0.004991332088853813,0.004491702276983852,0.004478087800476356,0.00452328183674508,0.004489201307304901,0.004545653333239627,0.004489573303017797,0.0044868147833230536])

     */


    val document = ds2.select("id","features").na.drop.rdd.map {
      case Row(id:Long, features: org.apache.spark.ml.linalg.Vector) =>  (id, Vectors.fromML(features))
    }

//    document.take(5).foreach(println)
    /*
    (0,(1820,[0,1,4,6,7,10,18,26,34,38,80,100,129,140,145,280,352,565,647,672,819,859,1136,1185],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
    (1,(1820,[0,9,16,20,23,30,33,36,41,47,55,63,67,69,74,97,128,156,189,355,397,503,643,753],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
    (2,(1820,[0,1,5,6,13,14,19,31,50,81,98,112,213,217,237,273,385,590,631,688,771,805,807,1300],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
    (3,(1820,[9,20,23,30,33,36,49,53,57,72,73,113,175,186,220,258,295,428,917,1679],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
    (4,(1820,[4,56,76,87,174,176,218,290,341,433,500,603,828,1169,1364,1413,1433,1556,1751],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))

     */

    val tfidf = document.map { case (i, v) => new IndexedRow(i, v) }
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
    ds4.printSchema()
//    ds4.show(5)



    /*
        val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
        //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量

        val tf_num_pairs = rdd1.map { x => {
          val tf = hashingTF.transform(x._5)
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
    */

    sc.stop()
  }
}
