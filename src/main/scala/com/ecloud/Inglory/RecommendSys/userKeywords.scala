package com.ecloud.Inglory.RecommendSys


import com.ecloud.Inglory.RecommendSys.alsDataProcessedV4.LogView
import com.ecloud.Inglory.RecommendSys.docSimsRecommend.UserProf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/10.
 * 读取getKeyWordsHbase输出的数据，与用户行为数据进行合并。
 * 将文章的标签打到用户上。
 */
object userKeywords {

  case class keywordsLab(val docID: String, val keywords: String, val keyword: String)
  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String)
  case class UserProf(
                       val userID: String,
                       val docID: String,
                       val value: Double) extends Serializable

  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    //bulid environment
    val spark = SparkSession.builder.appName("userKeywords").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数 t_keyWords
    //conf.set(TableInputFormat.INPUT_TABLE, "t_keyWords")

    //如果outputTable表存在，则删除表；如果不存在则新建表。
    val outputTable = args(1)
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable))
    {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(outputTable)
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("keyWords"))//keywords


    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val keywords = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("keyWords")) //keywords

      (rowkey, keywords)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val keywords_1 = Bytes.toString(x._2)

        (rowkey_1, keywords_1)
      }
      }.filter(x => x._2.length >= 2).flatMap{x =>
      val y = x._2.split(";").toIterable
      for (w <- y) yield (x._1, x._2, w)
    }.map(x => {
      val docID = x._1.toString
      val keywords = x._2.toString
      val keyword = x._3.toString
      keywordsLab(docID, keywords, keyword)
    })

    import spark.implicits._
    val rowkeyKeywordsDS = spark.createDataset(hbaseRDD)


    //read log files
    val logsRDD = sc.textFile("/app-ylzx-logs").filter(null != _)

    val logsRDD2 = logsRDD.map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView(line(4), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val logsRDD3 = logsRDD2.filter(x => x.REQUEST_URI.contains("search/getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        (userID, urlString)
      }).filter(_._1.length >= 10).filter(_._1 != null).map(x=>((x._1,x._2), 1)).reduceByKey(_ + _).map(x => {
      val userID = x._1._1
      val docID = x._1._2
      val value = x._2.toDouble
      UserProf(userID, docID, value)
    })

    val logsDS = spark.createDataset(logsRDD3)

    val joinDS = logsDS.join(rowkeyKeywordsDS, Seq("docID"), "left").na.drop().select("userID","keyword","value")
    import org.apache.spark.sql.functions._
    val dataPrep1 = joinDS.groupBy("userID","keyword").agg(sum(col("value"))).withColumnRenamed("sum(value)", "sum")
    val dataPrep2 = joinDS.groupBy("userID").agg(sum(col("value"))).withColumnRenamed("sum(value)", "total")
    val dataPrep3 = dataPrep1.join(dataPrep2, Seq("userID"), "left").na.drop()
    //useID, keyword,percent
    val dataPrep4 = dataPrep3.withColumn("percent", col("sum")/col("total"))

    //keyword排序
    val dataPrep5 = dataPrep4.groupBy("keyword").agg(sum("percent"))




  }
}
