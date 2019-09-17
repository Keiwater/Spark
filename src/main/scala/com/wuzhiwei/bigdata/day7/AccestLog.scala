package com.wuzhiwei.bigdata.day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AccestLog {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("AccestLog").master("local[*]").getOrCreate()

    // 读取访问日志
    val accessDS: Dataset[String] = spark.read.textFile(args(0))
    import spark.implicits._

    val ipDs: Dataset[(Long, String)] = accessDS.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      val time = fields(0).toLong
      (time, ip)
    })

    val ipDF: DataFrame = ipDs.toDF("time","ip")

    // 对数据进行整理压缩存储
    ipDF.write.mode("overwrite").parquet(args(1))







  }

}
