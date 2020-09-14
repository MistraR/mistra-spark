package com.mistra.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @author Mistra
 *         @ Version: 1.0
 *         @ Time: 2020/9/14 21:02
 *         @ Description:
 *         @ Copyright (c) Mistra,All Rights Reserved.
 *         @ Github: https://github.com/MistraR
 *         @ CSDN: https://blog.csdn.net/axela30w
 */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val sparkSession = SparkSession.builder().appName("Mistra").master("local[2]").getOrCreate()
    val peopleTable = sparkSession.read.json(args(0))
    peopleTable.show()


    // 关闭资源
    sparkSession.stop()
  }

}
