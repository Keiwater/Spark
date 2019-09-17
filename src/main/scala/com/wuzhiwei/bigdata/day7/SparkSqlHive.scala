package com.wuzhiwei.bigdata.day7

import org.apache.spark.sql.SparkSession

object SparkSqlHive {

  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder().appName("SparkSqlHive").master("local[*]")
      // 启动hive支持
      .enableHiveSupport()
//      .config("spark.sql.warehouse.dir","hdfs://172.16.10.187:9000/apps/hive/warehouse")
      .getOrCreate()

//    spark.sql("show databases").show()
//    spark.sql("select * from default.temp limit 100").show()

////
//    spark.sql("create temporary function  ip2Long as 'com.wuzhiwei.bigdata.hive.udf.udf.IPToLong' ")
//    spark.sql("select id,ip, ip2Long(ip) from access_log")

  }

}
