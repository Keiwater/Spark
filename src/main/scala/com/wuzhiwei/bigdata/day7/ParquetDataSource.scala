package com.wuzhiwei.bigdata.day7

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetDataSource").master("local[*]").getOrCreate()

//    val parquetData: DataFrame = spark.read.format("parquet").load("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\parquet")
    val parquetData: DataFrame = spark.read.format("orc").load("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\orc")

    parquetData.show()

  }
}
