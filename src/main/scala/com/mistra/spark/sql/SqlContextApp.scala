package com.mistra.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mistra
*         @ Version: 1.0
*         @ Time: 2020/9/14 20:44
*         @ Description:
*         @ Copyright (c) Mistra,All Rights Reserved.
*         @ Github: https://github.com/MistraR
*         @ CSDN: https://blog.csdn.net/axela30w
 */
object SqlContextApp {

  def main(args: Array[String]): Unit = {
    // 创建Context
    val sparkConf = new SparkConf().setAppName("Mistra").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load(args(0))
    people.printSchema()
    people.show()



    // 关闭资源
    sc.stop()
  }
}
