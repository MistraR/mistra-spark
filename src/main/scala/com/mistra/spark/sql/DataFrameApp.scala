package com.mistra.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @author Mistra
 *         @ Version: 1.0
 *         @ Time: 2020/9/14 21:42
 *         @ Description:
 *         @ Copyright (c) Mistra,All Rights Reserved.
 *         @ Github: https://github.com/MistraR
 *         @ CSDN: https://blog.csdn.net/axela30w
 */
object DataFrameApp {


  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.format("json").load("file:///Users/rocky/data/people.json")

    // 输出dataframe对应的schema信息
    peopleDF.printSchema()

    // 输出数据集的前20条记录
    peopleDF.show()

    //查询某列所有的数据： select name from table
    peopleDF.select("name").show()

    // 查询某几列所有的数据，并对列进行计算： select name, age+10 as age2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    //根据某一列的值进行过滤： select * from table where age>19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    //根据某一列进行分组，然后再进行聚合操作： select age,count(1) from table group by age
    peopleDF.groupBy("age").count().show()

    spark.stop()
  }

}
