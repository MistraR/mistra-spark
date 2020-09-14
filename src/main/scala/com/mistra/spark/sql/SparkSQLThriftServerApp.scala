package com.mistra.spark.sql

import java.sql.DriverManager

/**
 * @author Mistra
 *         @ Version: 1.0
 *         @ Time: 2020/9/14 21:24
 *         @ Description:
 *         @ Copyright (c) Mistra,All Rights Reserved.
 *         @ Github: https://github.com/MistraR
 *         @ CSDN: https://blog.csdn.net/axela30w
 */
object SparkSQLThriftServerApp {

  def main(args: Array[String]) {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://hadoop001:14000","hadoop","")
    val pstmt = conn.prepareStatement("select empno, ename, sal from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("empno:" + rs.getInt("empno") +
        " , ename:" + rs.getString("ename") +
        " , sal:" + rs.getDouble("sal"))

    }

    rs.close()
    pstmt.close()
    conn.close()


  }

}
