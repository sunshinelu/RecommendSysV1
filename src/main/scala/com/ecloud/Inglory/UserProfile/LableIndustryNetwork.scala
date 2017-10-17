package com.ecloud.Inglory.UserProfile

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/17.
 * 使用用户关注标签和用户所属行业构建标签和行业的网络关系图
 *
 * 用户标签数据：
 * YLZX_NRGL_OPER_CATEGORY
  OPERATOR_ID：操作员ID
  CATEGORY_NAME：栏目（标签）名称

OM_EMPLOYEE
 OPERATOR_ID：操作员主键
 INDUSTRY：行业

 */
object LableIndustryNetwork {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"LableIndustryNetwork") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val categoryTable = "YLZX_NRGL_OPER_CATEGORY"
    val industryTable = "OM_EMPLOYEE"

    val url1 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "ylzx")
    prop1.setProperty("password", "ylzx")
    //get data
    val category_df = spark.read.jdbc(url1, categoryTable, prop1)//.select("OPERATOR_ID", "CATEGORY_NAME")
    val industry_df = spark.read.jdbc(url1, industryTable, prop1).select("OPERATOR_ID", "INDUSTRY")

    // join category_df and industry_df based on OPERATOR_ID
    val df = category_df.join(industry_df, Seq("OPERATOR_ID"), "inner").withColumn("tag", lit(1))

    val df1 = df.groupBy("CATEGORY_NAME", "INDUSTRY").agg(sum("tag")).
      withColumnRenamed("sum(tag)", "weight")
    val categoryWeight = df.groupBy("CATEGORY_NAME").agg(sum("tag")).
      withColumnRenamed("sum(tag)", "weight")
    val industryWeight = df.groupBy("INDUSTRY").agg(sum("tag")).
      withColumnRenamed("sum(tag)", "weight")

    sc.stop()
    spark.stop()
  }

}
