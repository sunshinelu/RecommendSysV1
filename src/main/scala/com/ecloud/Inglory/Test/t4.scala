package com.ecloud.Inglory.Test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

/**
 * Created by sunlu on 17/2/16.
 */
object t4 {
  def main(args: Array[String]) {
    //定义时间格式
    val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 1
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    // cal.add(Calendar.YEAR, -N)//获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime
    //将时间戳转化为时间
    val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val date: String = sdf.format(new Date((todayL.toLong * 1000l)))


    println("========print(now)==========")
    println(now)

    println("========print(today)==========")
    println(today)

    println("========print(todayL)==========")
    println(todayL)

    println("========print(nDaysAgo)==========")
    println(nDaysAgo)

    println("========print(nDaysAgoL)==========")
    println(nDaysAgoL)

    println("========print(date)==========")
    println(date)
  }
}
