package com.ecloud.Inglory.Test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Created by sunlu on 17/4/26.
 */
object timeOpt {
  def main(args: Array[String]) {
    val t1 = "2017-04-26 16:43:05"

    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    println("tody value is: " + today)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    println("todayL value is: " + todayL)

    //val t1Date: Date = new Date(t1)
    //val t2 = dateFormat.format(t1Date)
    val t2 = dateFormat.parse(t1)
    println("t2 value is: " + t2) //t2的数据类型为时间类型
    val t2D = dateFormat.format(t2)
    println("t2L value is: " + t2D)
    val t2L = dateFormat2.parse(t2D).getTime
    println("t2L value is: " + t2L)

    val cal: Calendar = Calendar.getInstance()
    println(cal.getTime)

    val N = 10
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    //获取今天日期
    def getNowDate(): Long = {
      var now: Date = new Date()
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var today = dateFormat.format(now)
      val todayL = dateFormat.parse(today).getTime
      todayL
    }

    //获取昨天日期
    def getYesterday(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      var yesterday = dateFormat.format(cal.getTime())
      yesterday
    }

    //获取前天日期
    def get3Dasys(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -2)
      var threeDays = dateFormat.format(cal.getTime())
      threeDays
    }

    //获取一周前日期
    def get7Dasys(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -7)
      var sevenDays = dateFormat.format(cal.getTime())
      sevenDays
    }

    //获取半月前日期
    def getHalfMonth(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -15)
      var halfMonth = dateFormat.format(cal.getTime())
      halfMonth
    }


    //获取一月前日期
    def getOneMonth(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.MONTH, -1)
      var oneMonth = dateFormat.format(cal.getTime())
      oneMonth
    }

    //获取六月前日期
    def getSixMonth(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.MONTH, -6)
      var oneMonth = dateFormat.format(cal.getTime())
      oneMonth
    }

    //获取一年前日期
    def getOneYear(): String = {
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.YEAR, -1)
      var oneYear = dateFormat.format(cal.getTime())
      oneYear
    }



  }
}