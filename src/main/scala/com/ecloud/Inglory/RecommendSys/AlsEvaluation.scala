package com.ecloud.Inglory.RecommendSys

import java.io.File
import org.apache.log4j.{Level, Logger}

import scala.io.Source
import java.util.Properties

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.joda.time.{Duration, DateTime}


/**
 * Created by sunlu on 17/2/14.
 * ALS模型参数优化
 * 运行成功！
 */
object AlsEvaluation {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
  def PrepareData(tableName: String, userString: String, itemString: String): (RDD[Rating], RDD[Rating]) = {

    /*
    tableName:table name in MYSQL database
    userString: column name of user ID
    itemString: column name of item ID
     */
    //bulid environment
    val spark = SparkSession.builder.appName("AlsEvaluation").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //connect mysql database
    val url1 = "jdbc:mysql://192.168.37.26:3306/yeesotest"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    val df0 = spark.read.jdbc(url1, tableName, prop1) //tableName :"t_yhxw_log_prep"
    //userString:"emailid";itemString:"fullurl"
    val df1 = df0.withColumnRenamed(userString, "userString").withColumnRenamed(itemString, "itemString")
    val df2 = df1.groupBy("userString", "itemString").agg(count("userString")).withColumnRenamed("count(userString)", "VALUE")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(df2)
    val df3 = userID.transform(df2)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(df3)
    val df4 = urlID.transform(df3)
    //change data type
    val df5 = df4.withColumn("userID", df4("userID").cast("int")).withColumn("urlID", df4("urlID").cast("int")).withColumn("VALUE", df4("VALUE").cast("double"))
    //Min-Max Normalization[-1,1]
    val minMax = df5.agg(max("VALUE"), min("VALUE")).withColumnRenamed("max(VALUE)", "max").withColumnRenamed("min(VALUE)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val df6 = df5.withColumn("norm", bround((((df5("VALUE") - minValue) / (maxValue - minValue)) * 2 - 1), 4))

    //RDD to RowRDD
    val rdd1 = df6.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toInt
      val item = x._2.toString.toInt
      val rate = x._3.toString.toDouble
      Rating(user, item, rate)
    }

    val Array(trainData, validationData) = rdd1.randomSplit(Array(0.8, 0.12))

    return (trainData, validationData)
  }


  def trainValidation(trainData: RDD[Rating], validationData: RDD[Rating],
                      rankArray: Array[Int], numIterationsArray: Array[Int], lambdaArray: Array[Double]): MatrixFactorizationModel = {
    val evaluations =
      for (rank <- rankArray; numIterations <- numIterationsArray; lambda <- lambdaArray) yield {
        val (rmse, time) = trainModel(trainData, validationData, rank, numIterations, lambda)
        (rank, numIterations, lambda, rmse)
      }
    val Eval = (evaluations.sortBy(_._4))
    val BestEval = Eval(0)
    println("最佳model参数：rank:" + BestEval._1 + ",iterations:" + BestEval._2 + "lambda" + BestEval._3 + ",结果rmse = " + BestEval._4)
    val bestModel = ALS.train(trainData, BestEval._1, BestEval._2, BestEval._3)
    (bestModel)
  }

  def trainModel(trainData: RDD[Rating], validationData: RDD[Rating], rank: Int, iterations: Int, lambda: Double): (Double, Double) = {
    val startTime = new DateTime()
    val model = ALS.train(trainData, rank, iterations, lambda)
    val endTime = new DateTime()
    val Rmse = computeRMSE(model, validationData)
    val duration = new Duration(startTime, endTime)
    println(f"训练参数：rank:$rank%3d,iterations:$iterations%.2f ,lambda = $lambda%.2f 结果 Rmse=$Rmse%.2f" + "训练需要时间" + duration.getMillis + "毫秒")
    (Rmse, duration.getStandardSeconds)
  }

  def computeRMSE(model: MatrixFactorizationModel, RatingRDD: RDD[Rating]): Double = {

    val num = RatingRDD.count()
    val predictedRDD = model.predict(RatingRDD.map(r => (r.user, r.product)))
    val predictedAndRatings =
      predictedRDD.map(p => ((p.user, p.product), p.rating))
        .join(RatingRDD.map(r => ((r.user, r.product), r.rating)))
        .values
    math.sqrt(predictedAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / num)
  }

  def main(args: Array[String]) {
    SetLogger//不打印无用的LOG信息
    println("==========数据准备阶段===============")
    val (trainData, validationData) = PrepareData("t_yhxw_log_prep", "emailid", "fullurl")
    trainData.persist()
    validationData.persist()
    println("==========训练验证阶段===============")
    val bestModel = trainValidation(trainData, validationData, Array(5, 10, 15, 20, 25), Array(5, 10, 15, 20, 25), Array(0.05, 0.1, 1, 5, 10.0))
    println("==========测试阶段===============")
    val testRmse = computeRMSE(bestModel, validationData)
    println("使用testData测试bestModel," + "结果rmse = " + testRmse)
    trainData.unpersist()
    validationData.unpersist()


  }

}
