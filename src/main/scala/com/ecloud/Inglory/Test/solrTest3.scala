package com.ecloud.Inglory.Test

/**
 * Created by sunlu on 17/3/9.
 */
import java.util

import org.apache.solr.client.solrj.beans.Field
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.meta.field
/**
 * Created by qindongliang on 2016/1/21.
 */

//注册model,时间类型可以为字符串，只要后台索引配置为Long即可，注解映射形式如下
case class Record(
                   @(Field@field )("rowkey") rowkey:String,
                   @(Field@field )("title") title:String,
                   @(Field@field)("content") content:String,
                   @(Field@field)("isdel") isdel:String,
                   @(Field@field)("t1") t1:String,
                   @(Field@field)("t2")t2:String,
                   @(Field@field)("t3")t3:String,
                   @(Field@field)("dtime") dtime:String

                   )

/***
  * Spark构建索引==>Solr
  */
object solrTest3 {

  //solr客户端
  val client = new  HttpSolrClient("http://localhost:8983/solr/my_collection");
  //批提交的条数
  val batchCount=10000;

  def main(args: Array[String]) {

    val d1=new Record("row1","title","content","1","01","57","58","3");
    val d2=new Record("row2","title","content","1","01","57","58","45");
    val d3=new Record("row3","title","content","1","01","57","58",null);
    client.addBean(d1);
    client.addBean(d2)
    client.addBean(d3)
    client.commit();
    println("提交成功！")


  }


  /***
    * 迭代分区数据（一个迭代器集合），然后进行处理
    * @param lines 处理每个分区的数据
    */
  def  indexPartition(lines:scala.Iterator[String] ): Unit ={
    //初始化集合，分区迭代开始前，可以初始化一些内容，如数据库连接等
    val datas = new util.ArrayList[Record]()
    //迭代处理每条数据，符合条件会提交数据
    lines.foreach(line=>indexLineToModel(line,datas))
    //操作分区结束后，可以关闭一些资源，或者做一些操作，最后一次提交数据
    commitSolr(datas,true);
  }

  /***
    *  提交索引数据到solr中
    *
    * @param datas 索引数据
    * @param isEnd 是否为最后一次提交
    */
  def commitSolr(datas:util.ArrayList[Record],isEnd:Boolean): Unit ={
    //仅仅最后一次提交和集合长度等于批处理的数量时才提交
    if ((datas.size()>0&&isEnd)||datas.size()==batchCount) {
      client.addBeans(datas);
      client.commit(); //提交数据
      datas.clear();//清空集合，便于重用
    }
  }


  /***
    * 得到分区的数据具体每一行，并映射
    * 到Model，进行后续索引处理
    *
    * @param line 每行具体数据
    * @param datas 添加数据的集合，用于批量提交索引
    */
  def indexLineToModel(line:String,datas:util.ArrayList[Record]): Unit ={
    //数组数据清洗转换
    val fields=line.split("\1",-1).map(field =>etl_field(field))
    //将清洗完后的数组映射成Tuple类型
    val tuple=buildTuble(fields)
    //将Tuple转换成Bean类型
    val recoder=Record.tupled(tuple)
    //将实体类添加至集合，方便批处理提交
    datas.add(recoder);
    //提交索引到solr
    commitSolr(datas,false);
  }


  /***
    * 将数组映射成Tuple集合，方便与Bean绑定
    * @param array field集合数组
    * @return tuple集合
    */
  def buildTuble(array: Array[String]):(String, String, String, String, String, String, String, String)={
    array match {
      case Array(s1, s2, s3, s4, s5, s6, s7, s8) => (s1, s2, s3, s4, s5, s6, s7,s8)
    }
  }


  /***
    *  对field进行加工处理
    * 空值替换为null,这样索引里面就不会索引这个字段
    * ,正常值就还是原样返回
    *
    * @param field 用来走特定规则的数据
    * @return 映射完的数据
    */
  def etl_field(field:String):String={
    field match {
      case "" => null
      case _ => field
    }
  }

  /***
    * 根据条件清空某一类索引数据
    * @param query 删除的查询条件
    */
  def deleteSolrByQuery(query:String): Unit ={
    client.deleteByQuery(query);
    client.commit()
    println("删除成功!")
  }

/*
  def main(args: Array[String]) {
    //根据条件删除一些数据
    deleteSolrByQuery("t1:03")
    //远程提交时，需要提交打包后的jar
    val jarPath = "target\\spark-build-index-1.0-SNAPSHOT.jar";
    //远程提交时，伪装成相关的hadoop用户，否则，可能没有权限访问hdfs系统
    System.setProperty("user.name", "webmaster");
    //初始化SparkConf
    val conf = new SparkConf().setMaster("spark://192.168.1.187:7077").setAppName("build index ");
    //上传运行时依赖的jar包
    val seq = Seq(jarPath) :+ "D:\\tmp\\lib\\noggit-0.6.jar" :+ "D:\\tmp\\lib\\httpclient-4.3.1.jar" :+ "D:\\tmp\\lib\\httpcore-4.3.jar" :+ "D:\\tmp\\lib\\solr-solrj-5.1.0.jar" :+ "D:\\tmp\\lib\\httpmime-4.3.1.jar"
    conf.setJars(seq)
    //初始化SparkContext上下文
    val sc = new SparkContext(conf);
    //此目录下所有的数据，将会被构建索引,格式一定是约定好的
    val rdd = sc.textFile("hdfs://192.168.1.187:9000/user/monitor/gs/");
    //通过rdd构建索引
    indexRDD(rdd);
    //关闭索引资源
    client.close();
    //关闭SparkContext上下文
    sc.stop();


  }
*/

  /***
    * 处理rdd数据，构建索引
    * @param rdd
    */
  def indexRDD(rdd:RDD[String]): Unit ={
    //遍历分区，构建索引
    rdd.foreachPartition(line=>indexPartition(line));
  }



}