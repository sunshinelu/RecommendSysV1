package com.ecloud.Inglory.RecommendSys

import java.util.Properties
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/2/7.
 * 读取t_yhxw_log_prep表中数据构建基于物品的协同过滤模型，并将结果保存到mysql数据库t_yhxw_log_prep_norm_MavenTest表中
 * 代码运行成功！
 */
object ItemBasedRecommendT2 extends Serializable {
  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("ItemBasedRecommendT2").getOrCreate()
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
//make trainingset and testset
    var Array(trainingSet, testSet) = df6.randomSplit(Array(0.7, 0.3), 1234L)

//RDD to RowRDD
    val rdd1 = df6.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toLong
      val item = x._2.toString.toLong
      val rate = x._3.toString.toDouble
      MatrixEntry(user, item, rate)
    }
//calculate similarities
    val ratings = new CoordinateMatrix(rdd1)
    val itemSimi = ratings.toRowMatrix.columnSimilarities(0.1)

    /**
     * 相似度.
     * @param itemid1 物品
     * @param itemid2 物品
     * @param similar 相似度
     */
    case class ItemSimi(
                         val itemid1: Long,
                         val itemid2: Long,
                         val similar: Double
                         ) extends Serializable
    /**
     * 用户推荐.
     * @param userid 用户
     * @param itemid 推荐物品
     * @param pref 评分
     */
    case class UserRecomm(
                           val userid: Long,
                           val itemid: Long,
                           val pref: Double
                           ) extends Serializable

    val itemSimiRdd = itemSimi.entries.map(f => ItemSimi(f.i, f.j, f.value))

    val rdd_app_R1 = itemSimiRdd.map { f => (f.itemid1, f.itemid2, f.similar) }
    val user_prefer1 = rdd1.map { f => (f.i, f.j, f.value) }

    val rdd_app_R2 = rdd_app_R1.map { f => (f._1, (f._2, f._3)) }.join(user_prefer1.map(f => (f._2, (f._1, f._3))))
    val rdd_app_R3 = rdd_app_R2.map { f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2) }
    val rdd_app_R4 = rdd_app_R3.reduceByKey((x, y) => x + y)
    val rdd_app_R5 = rdd_app_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))

    val rdd_app_R6 = rdd_app_R5.groupByKey()
    val r_number = 30
    val rdd_app_R7 = rdd_app_R6.map { f =>
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    }

    val rdd_app_R8 = rdd_app_R7.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    }
    )
// make schema
    val schema = StructType(
      StructField("userid", LongType) ::
        StructField("itemid", LongType) ::
        StructField("pref", DoubleType)
        :: Nil)
//RDD to RowRDD
    val itemRecomm = rdd_app_R8.map { f => Row(f._1, f._2, f._3) }
    //RowRDD to DF
    val itemRecomDF = spark.createDataFrame(itemRecomm, schema)


    val userLab = df6.select("emailid", "userID").dropDuplicates
    val itemLab = df6.select("fullurl", "urlID").dropDuplicates

    val joinDF1 = itemRecomDF.join(userLab, Seq("userid"), "left")
    val joinDF2 = joinDF1.join(itemLab, itemRecomDF("itemid") === itemLab("urlID"), "left").drop("urlID")

    //将joinedDF2保存到t_fzb_url表中
    val url2 = "jdbc:mysql://192.168.37.26:3306/yeesotest?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //将结果保存到数据框中
    joinDF2.write.mode("overwrite").jdbc(url2, "t_yhxw_log_prep_norm_MavenTest", prop2)
    sc.stop()

  }
}
