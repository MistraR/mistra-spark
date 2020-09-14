package com.mistra.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mistra
 *         @ Version: 1.0
 *         @ Time: 2020/9/14 20:57
 *         @ Description:
 *         @ Copyright (c) Mistra,All Rights Reserved.
 *         @ Github: https://github.com/MistraR
 *         @ CSDN: https://blog.csdn.net/axela30w
 */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    // 创建Context
    val sparkConf = new SparkConf().setAppName("Mistra").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    // 读取hive中的表数据
    hiveContext.table("emp").show()

    // 关闭资源
    sc.stop()
  }

}
