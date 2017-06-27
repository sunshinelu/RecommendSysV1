package com.ecloud.Inglory.Test

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue

/**
  * Created by sunlu on 17/4/14.
  */
object ansjLoadLib {
   def main(args: Array[String]) {
     //在用词典未加载前可以通过,代码方式方式来加载
     MyStaticValue.userLibrary = "library/userDefine.dic"

     val s1 = "大数据时代来了，你想好要做什么了吗？"
     val seg = ToAnalysis.parse(s1)
     println(seg)
   }
 }
