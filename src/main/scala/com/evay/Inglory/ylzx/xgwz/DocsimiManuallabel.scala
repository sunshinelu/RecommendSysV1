package com.evay.Inglory.ylzx.xgwz


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/29.
 * 使用yilan-total_webpage表中的manuallabel列计算文章的相似性
 */
object DocsimiManuallabel {

  case class DocSimiId(doc1Id:Long, doc2Id: Long, value: Double)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(s"DocsimiManuallabel") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val docSimiTable = args(1)

    val ylzxRDD = xgwzUtil.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)//.randomSplit(Array(0.01, 0.99))(0)
//  case class YlzxSchema(itemString: String, title: String, manuallabel: String, time: String,timeL: Long, websitename: String, content: String)

    val ds1 = ylzxDS.drop("content").
      withColumn("labelWord", explode(split($"manuallabel", ";"))).withColumn("value", lit(1.0))

    //string to number
    val docID = new StringIndexer().setInputCol("itemString").setOutputCol("docID").fit(ds1)
    val ds2 = docID.transform(ds1)
    val wordID = new StringIndexer().setInputCol("labelWord").setOutputCol("wordID").fit(ds2)
    val ds3 = wordID.transform(ds2)
    val ds4 = ds3.withColumn("docID", ds3("docID").cast("long")).
      withColumn("wordID", ds3("wordID").cast("long")).
      withColumn("value", ds3("value").cast("double"))

    //RDD to RowRDD
    val rdd1 = ds4.select("wordID","docID", "value").rdd.map { case Row(wordID: Long, docID:Long, value: Double) => MatrixEntry(wordID, docID, value) }

    //calculate similarities
    val mat = new CoordinateMatrix(rdd1)
    val docSimi = mat.toRowMatrix.columnSimilarities(0.1)
    val docSimiRdd = docSimi.entries.map(f => DocSimiId(f.i, f.j, f.value)).
      union(docSimi.entries.map(f => DocSimiId(f.j, f.i, f.value)))

    val docSimiDS = spark.createDataset(docSimiRdd)

    val doc1Lab = ds4.select("itemString", "docID").
      withColumnRenamed("itemString", "doc1").withColumnRenamed("docID", "doc1ID")
    val doc2Lab = ds4.select("itemString", "docID","title", "manuallabel","time", "timeL", "websitename").
      withColumnRenamed("itemString", "doc2").withColumnRenamed("docID", "doc2ID")

    val joinedDS = docSimiDS.join(doc1Lab, Seq("doc1ID"), "left").join(doc2Lab, Seq("doc2ID"), "left").na.drop()

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("doc1").orderBy(col("timeL").desc)
    val filterDS = joinedDS.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val resultDS = filterDS.select("doc1", "doc2", "value", "rn", "title", "manuallabel", "time", "websitename")

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    //    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    /*
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
//      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
*/

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(docSimiTable)) {
      hAdmin.disableTable(docSimiTable)
      hAdmin.deleteTable(docSimiTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(docSimiTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, docSimiTable) //设置输出表名


    //    val table = new HTable(hbaseConf,docSimiTable)
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,docSimiTable)

    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    resultDS.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map { x => {
        //("doc1","doc2", "value", "rn","title","label","time","websitename")
        val paste = x._1.toString + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //doc1
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._2.toString)) //doc2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._3.toString)) //value
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._5.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._8.toString)) //websitename2

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)





    sc.stop()
    spark.stop()
  }
}
