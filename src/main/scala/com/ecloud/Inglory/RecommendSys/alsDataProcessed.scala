package com.ecloud.Inglory.RecommendSys

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/2/9.
 * 使用mysql数据库中t_yhxw_log_prep表中数据构建ALS推荐模型
 * 与HBASE表yeeso_webpage中的标题、URL、标签进行join操作。
 * 运行成功！
 */
object alsDataProcessed {
  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("alsDataProcessed").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //connect mysql database
    val url1 = "jdbc:mysql://192.168.37.26:3306/yeesotest"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    val df1 = spark.read.jdbc(url1, "t_yhxw_log_prep", prop1)
    val df2 = df1.groupBy("emailid", "fullurl").agg(count("emailid")).withColumnRenamed("count(emailid)", "VALUE")
    //string to number
    val userID = new StringIndexer().setInputCol("emailid").setOutputCol("userID").fit(df2)
    val df3 = userID.transform(df2)
    val urlID = new StringIndexer().setInputCol("fullurl").setOutputCol("urlID").fit(df3)
    val df4 = urlID.transform(df3)
    //change data type
    val df5 = df4.withColumn("userID", df4("userID").cast("long")).withColumn("urlID", df4("urlID").cast("long")).withColumn("VALUE", df4("VALUE").cast("double"))
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
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(rdd1, rank, numIterations, 0.01)
    //防止路径下该文件夹存在
    val filepath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }

    // Save and load model
    model.save(sc, "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS")//没有overwrite功能

    val sameModel = MatrixFactorizationModel.load(sc, "hdfs://192.168.37.21:8020/personal/sunlu/lulu/yeeso/t_yhxw_log_prep_ALS")

    val topProducts = model.recommendProductsForUsers(10)

    // make schema
    val schema = StructType(
      StructField("userID", LongType) ::
        StructField("urlID", LongType) ::
        StructField("rating", DoubleType)
        :: Nil)
    //RDD to RowRDD
    val topProductsRowRDD =  topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield ( w.user, w.product, w.rating)
    }).map { f => Row(f._1.toLong, f._2.toLong, f._3) }

    //RowRDD to DF
    val topProductsDF = spark.createDataFrame(topProductsRowRDD, schema)


    val userLab = df6.select("emailid", "userID").dropDuplicates
    val itemLab = df6.select("fullurl", "urlID").dropDuplicates

    val joinDF1 = topProductsDF.join(userLab, Seq("userID"), "left")
    //val joinDF2 = joinDF1.join(itemLab, topProductsDF("itemid") === itemLab("urlID"), "left").drop("urlID")
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left")


    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "yeeso_webpage") //设置输入表名 第一个参数yeeso_webpage或ywkNew_webpage
    //扫描整个表
    val scan = new Scan()
    //获得Hbase表的记录，每条记录的格式<key,value>=<ImmutableBytesWritable,Result>
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseDate = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"))//标题列
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"))//URL列
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("lab"))//标签列
      val time = v.getValue(Bytes.toBytes("h"), Bytes.toBytes("Last-Modified"))//时间列
      (key, title, url, label, time)
    }
    }.filter(x => null != x._3 ).//URL列不为空
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val url_1 = Bytes.toString(x._3)
        val label_1 = Bytes.toString(x._4)
        val time_1 = Bytes.toString(x._5)
        (rowkey_1, title_1, url_1, label_1, time_1)
      }
      }//.filter(x => x._3.length > 0 & x._4.length > 0 & x._5.length > 0)

    // make schema2
    val schema2 = StructType(
      StructField("rowkey", StringType) ::
        StructField("title", StringType) ::
        StructField("fullurl", StringType) ::
        StructField("label", StringType) ::
        StructField("time", StringType)
        :: Nil)

    val yeesoDateRowRDD = hbaseDate.map { f => Row(f._1, f._2, f._3, f._4, f._5) }
    //RowRDD to DF
    val yeesoDateDF = spark.createDataFrame(yeesoDateRowRDD, schema2)
    val joinDF3 = joinDF2.join(yeesoDateDF, Seq("fullurl"), "left")


    //将joinedDF2保存到t_fzb_url表中
    val url2 = "jdbc:mysql://192.168.37.26:3306/yeesotest?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    joinDF3.write.mode("overwrite").jdbc(url2, "t_yhxw_log_prep_ALSprocessed", prop2)
    sc.stop()
  }
}
