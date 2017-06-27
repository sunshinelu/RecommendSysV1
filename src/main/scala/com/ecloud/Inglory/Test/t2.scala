package com.ecloud.Inglory.Test

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

/**
 * Created by sunlu on 17/2/14.
 */
object t2 {

  def main(args: Array[String]) {
    val reg2 = """(\w+\.){2}\w+.*,""".r
    val s1 = "id=cn.gov.most.www:http/tztg/201702/t20170222_131121.htm, COMMENTCUPAGE=1"
    val test1 = reg2.findFirstIn(s1)
    val test2 = test1.get
    test1.foreach(println)
    println(test1)
    println("=======")
    println(test2)

  }
}
