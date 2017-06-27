package com.ecloud.Inglory.RecommendSys

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale, Properties}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/3/2.
 * 热门标签Top20
 * 读取SYS_LOG中的数据，并提取keyword，对keyword进行word count，取TOP20输出到mysql数据库中。
 * 数据保存为时间列和hotwords，存储模式为append
 * 程序运行成功！时间（2017年03月02日）
 * 弃用该方法（弃用原因：与需求不一致！！）
 */
object hotLabels {
  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("hotLabels").getOrCreate()
    val sc = spark.sparkContext

    //connect mysql database
    val url1 = "jdbc:mysql://192.168.37.18:3306/recommender_test"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val df1 = spark.read.jdbc(url1, "SYS_LOG", prop1)

    val df2 = df1.select("CREATE_BY","CREATE_BY_ID","REQUEST_URI","PARAMS")
    //过滤REQUEST_URI列中包含searchByKeyword.do的列，并提取PARAMS中的keyword
    val df3 = df2.filter(df2("REQUEST_URI").contains("searchByKeyword.do"))
      val rdd1 = df3.rdd.map{row => (row(0), row(1), row(2), row(3))}.map(x => {
      //val userName = x._1.toString
      val userID = x._2.toString
      val reg = """keyword=.+(,|})""".r
      val keyword = reg.findFirstIn(x._4.toString).get.replace("keyword=","").replace(",","").replace("}","")
      (userID, keyword)
    })
    val rdd2 = rdd1.map(x=>(x._2, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(20)
    val rdd3 = rdd2.map(_._1).mkString(";")

    //定义时间格式
//    val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //获取当前时间
    val now: Date = new Date()
    //对时间格式进行格式化
    val rightNow = dateFormat.format(now)
    val rdd4 = sc.parallelize(List((rightNow,rdd3)).map(x => Row(x._1, x._2)))
    val schema = StructType(
      StructField("time", StringType) ::
        StructField("hotWords", StringType)
        :: Nil)
    val df4 = spark.createDataFrame(rdd4, schema)
    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    df4.write.mode("append").jdbc(url2, "hotWords_Test", prop2)//overwrite

    sc.stop()
    spark.stop()

  }


}
