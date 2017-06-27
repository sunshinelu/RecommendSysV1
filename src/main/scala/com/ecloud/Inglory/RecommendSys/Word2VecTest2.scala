package com.ecloud.Inglory.RecommendSys

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/2/28.
 */
object Word2VecTest2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName("Word2VecTest2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("file:///Users/sunlu/Software/spark-2.0.2-bin-hadoop2.6/data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    val modelPath = "file:///Users/sunlu/Desktop/Test"
    model.save(sc, modelPath)
    val sameModel = Word2VecModel.load(sc, modelPath)
    val synonyms2 = sameModel.findSynonyms("1", 5)
    val temp = synonyms2.map(_._1).mkString(";")
    println("=======")
    println(temp)

  }
}
