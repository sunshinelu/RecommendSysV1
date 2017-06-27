package com.ecloud.Inglory.Test

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue

/**
 * Created by sunlu on 17/2/27.
 */
object DynamicWordDemo {
  def main(args: Array[String]) {
    // 增加新词,中间按照'\t'隔开
    UserDefineLibrary.insertWord("ansj中文分词", "userDefine", 1000)
   val terms1 = ToAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!")

    println("增加新词例子:" + terms1)
    // 删除词语,只能删除.用户自定义的词典.
    UserDefineLibrary.removeWord("ansj中文分词")
    val terms2 = ToAnalysis.parse("我觉得ansj中文分词是一个不错的系统!我是王婆!")
    println("删除用户自定义词典例子:" + terms2)

    //在用词典未加载前可以通过,代码方式方式来加载
    MyStaticValue.userLibrary = "library/userDefine.dic"

    //第三种,调用api加载.在程序运行的任何时间都可以.动态价值

    //loadLibrary.loadLibrary(String "path")
      //路径可以是具体文件也可以是一个目录 如果是一个目录.那么会扫描目录下的dic文件自动加入词典

  }
}
