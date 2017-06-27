package com.ecloud.Inglory.Test

import java.nio.charset.Charset

/**
 * Created by sunlu on 17/4/8.
 */
object defaultCharset {
  def main(args: Array[String]) {
    println("JVM的默认编码为：" + Charset.defaultCharset())
    println(System.getProperty("file.encoding"))
  }
}
