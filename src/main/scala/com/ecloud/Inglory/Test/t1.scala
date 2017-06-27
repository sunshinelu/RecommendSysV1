package com.ecloud.Inglory.Test

import java.nio.ByteBuffer

import org.ansj.app.keyword.KeyWordComputer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/2/9.
 */
object t1 {
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("ContentBasedRecommendMethod1").setExecutorEnv("", "")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("fs.defaultFS", "hdfs://192.168.37.21:8020")
    conf.set("mapreduce.framework.name", "yarn")
    conf.set("yarn.resourcemanager.cluster-id", "yarn-cluster")
    conf.set("yarn.resourcemanager.hostname", "192.168.37.22", "192.168.37.25")
    conf.set("yarn.resourcemanager.admin.address", "192.168.37.25:8141")
    conf.set("yarn.resourcemanager.address", "192.168.37.25:8050")
    conf.set("yarn.resourcemanager.resource-tracker.address", "1192.168.37.25:8025")
    conf.set("yarn.resourcemanager.scheduler.address", "192.168.37.25:8030")

    conf.set("hbase.master", "192.168.37.21:60000")
    conf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.nameserver.address", "192.168.37.21,192.168.37.22")
    conf.set("hbase.regionserver.dns.nameserver",
      "192.168.37.22,192.168.37.23,192.168.37.24")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "t_yeeso100") //设置输入表名 第一个参数t_yeeso100

    /*
    val config = new Configuration()
    config.set("fs.defaultFS", "hdfs://192.168.37.21:8020")
    config.set("yarn.resourcemanager.cluster-id","yarn-cluster")
    config.set("yarn.resourcemanager.hostname", "192.168.37.22", "192.168.37.25")
    config.set("yarn.resourcemanager.admin.address", "192.168.37.25:8141")
    config.set("yarn.resourcemanager.address", "192.168.37.25:8050")
    config.set("yarn.resourcemanager.resource-tracker.address", "1192.168.37.25:8025")
    config.set("yarn.resourcemanager.scheduler.address", "192.168.37.25:8030")
*/
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"))
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //val table = new HTable(conf,"t_yeeso100")
    //val scanner = table.getScanner(scan)


    //提取亿搜数据，并对数据进行过滤
    val hbaseDate = hBaseRDD.map { case (k, v) => {
      val key = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"))
      val url = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"))
      (key, title, url)
    }
    }.filter(x => null != x._2 & null != x._3).
    map { x => {
      val rowkey_1 = Bytes.toString(x._1)
      val title_1 = Bytes.toString(x._2)
      val url_1 = Bytes.toString(x._3)
      (rowkey_1, title_1, url_1)
    }}.filter(x => x._2.length > 0 & x._3.length > 0)

  //防止路径下该文件夹存在
    val filepath = "hdfs://192.168.37.21:8020/personal/sunlu/lulu/hbase_t1"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.37.21:8020"), hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true)
    } catch {
      case _: Throwable => {}
    }

    hbaseDate.saveAsTextFile(filepath)
    /*
        val fams = Array("", "")
        val quals = Array("", "")
        for (fam <- fams) {
          for (qual <- quals) {
            scan.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
          }
          */


    /*
  //测试关键词提取方法
  val kwc = new KeyWordComputer(5)
  val title = "维基解密否认斯诺登接受委内瑞拉庇护"
  val content = "有俄罗斯国会议员，9号在社交网站推特表示，美国中情局前雇员斯诺登，已经接受委内瑞拉的庇护，不过推文在发布几分钟后随即删除。俄罗斯当局拒绝发表评论，而一直协助斯诺登的维基解密否认他将投靠委内瑞拉。　　俄罗斯国会国际事务委员会主席普什科夫，在个人推特率先披露斯诺登已接受委内瑞拉的庇护建议，令外界以为斯诺登的动向终于有新进展。　　不过推文在几分钟内旋即被删除，普什科夫澄清他是看到俄罗斯国营电视台的新闻才这样说，而电视台已经作出否认，称普什科夫是误解了新闻内容。　　委内瑞拉驻莫斯科大使馆、俄罗斯总统府发言人、以及外交部都拒绝发表评论。而维基解密就否认斯诺登已正式接受委内瑞拉的庇护，说会在适当时间公布有关决定。　　斯诺登相信目前还在莫斯科谢列梅捷沃机场，已滞留两个多星期。他早前向约20个国家提交庇护申请，委内瑞拉、尼加拉瓜和玻利维亚，先后表示答应，不过斯诺登还没作出决定。　　而另一场外交风波，玻利维亚总统莫拉莱斯的专机上星期被欧洲多国以怀疑斯诺登在机上为由拒绝过境事件，涉事国家之一的西班牙突然转口风，外长马加略]号表示愿意就任何误解致歉，但强调当时当局没有关闭领空或不许专机降落。"
  val result = kwc.computeArticleTfidf(title, content)
  println(result)
  */
  }

}
