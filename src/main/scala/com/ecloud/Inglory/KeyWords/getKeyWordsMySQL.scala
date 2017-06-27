package com.ecloud.Inglory.KeyWords

import java.util.Properties

import org.ansj.app.keyword.KeyWordComputer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/5.
 */


object getKeyWordsMySQL {


  case class Schema(id: String, keywords: String, title: String, content: String)

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }
  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("getKeyWordsMySQL").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/yiyun?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val df1 = spark.read.jdbc(url1, "policy", prop1).select("id", "title", "content")
    val rdd1 = df1.rdd.map(row => (row(0), row(1), row(2))).map(x => {
      val id = x._1.toString
      val title = x._2.toString
      val content = x._3.toString

      val kwc = new KeyWordComputer(5)
      val result = kwc.computeArticleTfidf(title, content).toArray
      val keywords = for (ele <- result) yield ele.toString.split("/")(0)
      val keywordsString = keywords.mkString(";")
      Schema(id, keywordsString, title, content)
    })
    import spark.implicits._
    val df2 = spark.createDataset(rdd1)
    df1.unpersist()
    rdd1.unpersist()
    df2.persist()

    val df3 = spark.read.jdbc(url1, "word", prop1).select("text_name", "vipwords")
    val df4 = df2.join(df3, df2("id") === df3("text_name"), "inner").drop("text_name")


    //将joinedDF4保存到als_Test表中
    val url2 = "jdbc:mysql://localhost:3306/yiyun?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    df4.write.mode("overwrite").jdbc(url2, "ansj_keyWords", prop2)
    sc.stop()
  }
}
