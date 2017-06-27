package com.ecloud.Inglory.Test

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunlu on 17/3/6.
 */
object Springer_author {
  def main(args: Array[String]) {
    val url = "jdbc:mysql://192.168.37.19:3306/yeerc?user=root&password=123456&useUnicode=true&characterEncoding=utf-8"
    val sparkConf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "springer_parse")
    import sqlContext.implicits._
    val scan = new Scan()
    //获得Hbase表的记录，每条记录的格式<key,value>=<ImmutableBytesWritable,Result>
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //取得result，tuple=>tuple._2 第二个元素就是result
    val data = hBaseRDD.map(x=>x._2)
      .map{x=>(x.getValue(Bytes.toBytes("info"), Bytes.toBytes("author_Affiliation")))}
      .filter(x => null != x)
      .map(x => Bytes.toString(x).trim)
      .filter(x => x.length > 1)
      .map(x=> x.split(";").toList)
      .filter(x=> x.size>1)
      .flatMap(x => x.combinations(2))
      .map(x => (x(0), x(1)))
      .distinct()


    val data1= data.toDF("user1", "user2")

    //创建Properties存储数据库相关属性
    val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")
    //将数据追加到数据库
    data1.write.mode("append").jdbc("jdbc:mysql://192.168.37.19:3306/yeerc?useUnicode=true&characterEncoding=UTF-8", "test_author1", prop)

    sc.stop()
  }
}
