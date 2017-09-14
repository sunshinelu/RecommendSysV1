时间：2017年09月11日

运行CountLabel



spark-submit \
--class com.ecloud.Inglory.UserProfile.CountLabel \
--master yarn \
--num-executors 4 \
--executor-cores 4 \
--executor-memory 2g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Workspace/spark/yeeso/RecommendSys/RecommendSysV1.jar \
yilan-total_webpage t_hbaseSink


spark-shell下测试：

spark-shell --master yarn --num-executors 4 --executor-cores  2 --executor-memory 4g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class LogView(CREATE_BY_ID: String, CREATE_TIME_L: Long, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: String, value: Double)


def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

    // 获取时间
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 2
      cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    // cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      (userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val creatTime = Bytes.toString(x._2)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat2.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, creatTimeS, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
    ).filter(_.CREATE_TIME_L >= nDaysAgoL).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        //        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 =
          """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        val time = x.CREATE_TIME
        val value = 1.0
        val rating = x.REQUEST_URI match {
          case r if (r.contains("getContentById.do")) => 1.0 * value //0.2
          case r if (r.contains("like/add.do")) => 1.0 * value //0.3
          case r if (r.contains("favorite/add.do")) => 1.0 * value //0.5
          case r if (r.contains("addFavorite.do")) => 1.0 * value //0.5
          case r if (r.contains("favorite/delete.do")) => -1.0 * value //-0.5
          case r if (r.contains("delFavorite.do")) => -1.0 * value //-0.5
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5)

    hbaseRDD
  }

 case class LabelView(itemString: String, manuallabel: String, webLabel: String, webName: String, dist: String)

def getLabelsRDD(ylzxTable: String, sc: SparkContext): RDD[LabelView] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名称
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val webLabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名称
      val dist = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("xzqhname")) //行政区划
      (urlID, manuallabel, webLabel, webName, dist)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 = Bytes.toString(x._2)
        val webLabel_1 = Bytes.toString(x._3)
        val webName_1 = Bytes.toString(x._4)
        val dist_1 = Bytes.toString(x._5)
        LabelView(urlID_1, manuallabel_1, webLabel_1, webName_1, dist_1)
      }
      ).filter(_.manuallabel.length >= 2)
    /*.flatMap {
    case (id, manuallabel) => manuallabel.split(";").map((id, _))
  }.map(x => {
    val rowkey = x._1
    val label = x._2
    val value = 1.0
    LabelView(rowkey, label, value)
  })
*/
    hbaseRDD
  }

case class WZBQTJschema(YHID: String, WZBQTJ: String)

  //网站标签
  case class XZQHTJschema(YHID: String, XZQHTJ: String)

  //行政区划
  case class WZLBTJschema(YHID: String, WZLBTJ: String)

  //网站类别
  case class WZTJschema(YHID: String, WZTJ: String)

  //网站名称


val ylzxTable = "yilan-total_webpage"
val logsTable = "t_hbaseSink"

val logsRDD = getLogsRDD(logsTable, sc)
val logsDS = spark.createDataset(logsRDD).groupBy("userString", "itemString").agg(sum("value")).drop("value").
      withColumnRenamed("sum(value)", "value")