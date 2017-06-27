package com.ecloud.Inglory.RecommendSys

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/2/20.
 */
object alsTest {
  def main(args: Array[String]) {

    //bulid environment
    val spark = SparkSession.builder.appName("alsTest").getOrCreate()
    val sc = spark.sparkContext

    val alsModel = MatrixFactorizationModel.load(sc, "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS")

    val userID = 2
    val topN = 10
    val test1 = alsModel.recommendProducts(userID, topN)

    test1.foreach(println)
  }
}
