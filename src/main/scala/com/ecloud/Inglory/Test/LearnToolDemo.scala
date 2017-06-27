package com.ecloud.Inglory.Test

import org.ansj.dic.LearnTool
import org.ansj.domain.Nature
import org.ansj.splitWord.analysis.NlpAnalysis

/**
 * Created by sunlu on 17/4/14.
 */
object LearnToolDemo {
  def main(args: Array[String]) {
    //构建一个新词学习的工具类。这个对象。保存了所有分词中出现的新词。出现次数越多。相对权重越大。
    val learnTool = new LearnTool()

    //进行词语分词。也就是nlp方式分词，这里可以分多篇文章
    NlpAnalysis.parse("说过，社交软件也是打着沟通的平台，让无数寂寞男女有了肉体与精神的寄托。", learnTool) ;
    NlpAnalysis.parse("其实可以打着这个需求点去运作的互联网公司不应只是社交类软件与可穿戴设备，还有携程网，去哪儿网等等，订房订酒店多好的寓意", learnTool) ;
    NlpAnalysis.parse("张艺谋的卡宴，马明哲的戏",learnTool)

    //取得学习到的topn新词,返回前10个。这里如果设置为0则返回全部
    println(learnTool.getTopTree(0))

    //只取得词性为Nature.NR的新词
    println(learnTool.getTopTree(10,Nature.NR))
  }
}
