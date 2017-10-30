package com.ecloud.Inglory.RatingSys

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Created by sunlu on 17/4/26.
 */
object UtilTool extends Serializable{

/*
  val today = UtilTool.getNowDate()
  val threeDay = UtilTool.get3Dasys()
  val sevenDay = UtilTool.get7Dasys()
  val halfMonth = UtilTool.getHalfMonth()
  val oneMonth = UtilTool.getOneMonth()
  val sixMonth = UtilTool.getSixMonth()
  val oneYear = UtilTool.getOneYear()
*/


  /*
    val tool = new UtilTool
  val today = tool.getNowDate()
  val threeDay = tool.get3Dasys()
  val sevenDay = tool.get7Dasys()
  val halfMonth = tool.getHalfMonth()
  val oneMonth = tool.getOneMonth()
  val sixMonth = tool.getSixMonth()
  val oneYear = tool.getOneYear()
   */
  //获取明天日期
  def getTomorrow(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 1)
    var tomorrow = dateFormat.format(cal.getTime())
    val tomorrowL = dateFormat.parse(tomorrow).getTime
    tomorrowL
  }

  //获取今天日期
  def getNowDate(): Long = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var today = dateFormat.format(now)
    val todayL = dateFormat.parse(today).getTime
    todayL
  }

  //获取昨天日期
  def getYesterday(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    val yesterdayL = dateFormat.parse(yesterday).getTime
    yesterdayL
  }

  //获取前天日期
  def get3Dasys(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    var threeDays = dateFormat.format(cal.getTime())
    val threeDaysL = dateFormat.parse(threeDays).getTime
    threeDaysL
  }

  //获取一周前日期
  def get7Dasys(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -7)
    var sevenDays = dateFormat.format(cal.getTime())
    val sevenDaysL = dateFormat.parse(sevenDays).getTime
    sevenDaysL
  }

  //获取半月前日期
  def getHalfMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -15)
    var halfMonth = dateFormat.format(cal.getTime())
    val halfMonthL = dateFormat.parse(halfMonth).getTime
    halfMonthL
  }


  //获取一月前日期
  def getOneMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -1)
    var oneMonth = dateFormat.format(cal.getTime())
    val oneMonthL = dateFormat.parse(oneMonth).getTime
    oneMonthL
  }

  //获取六月前日期
  def getSixMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -6)
    var sixMonth = dateFormat.format(cal.getTime())
    val siMonthL = dateFormat.parse(sixMonth).getTime
    siMonthL
  }

  //获取一年前日期
  def getOneYear(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.YEAR, -1)
    var oneYear = dateFormat.format(cal.getTime())
    val oneYearL = dateFormat.parse(oneYear).getTime
    oneYearL
  }
}
