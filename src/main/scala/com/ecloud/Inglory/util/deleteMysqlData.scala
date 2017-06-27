package com.ecloud.Inglory.util

import java.sql.{SQLException, DriverManager}

/**
 * Created by sunlu on 17/6/26.
 */
object deleteMysqlData {

  def deleteMysqlData(url: String, user: String, password:String, tableName:String, timeString: String) : Unit ={
    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"

    try {
      // 加载驱动程序
      Class.forName(driver)
      // 连续数据库
      val conn = DriverManager.getConnection(url, user, password)
      if (!conn.isClosed())
        System.out.println("Succeeded connecting to the Database!")
      // statement用来执行SQL语句
      val statement = conn.createStatement()
      // 要执行的SQL语句
//      val sql = "truncate table " + tableName
      val sql = "delete from " + tableName + " where CREATE_DATE < '" + timeString + "'"
      //val sql2 = "delete from ylzx_oper_interest where CREATE_DATE < '2017-06-26 10:15:15'"
      val rs = statement.executeUpdate(sql)
      println("delete data succeeded!")
    } catch {
      case ex: ClassNotFoundException => {
        println("Sorry,can`t find the Driver!")
        ex.printStackTrace()
      }
      case ex: SQLException =>{
        ex.printStackTrace()
      }
      case ex: Exception => {
        ex.printStackTrace()
      }

    }

  }

}
