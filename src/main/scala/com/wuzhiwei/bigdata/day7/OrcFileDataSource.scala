package com.wuzhiwei.bigdata.day7

import org.apache.spark.sql.{DataFrame, SparkSession}

object OrcFileDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("OrcFileDataSource").master("local[*]").getOrCreate()

//    val parquetData: DataFrame = spark.read.format("parquet").load("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\parquet")
    val orcData: DataFrame = spark.read.format("orc").load("hdfs://172.16.10.187:9000/apps/hive/warehouse/tw_loa.db/tw_loa_res_acc_se_new_product_rate/bdw_statis_year=2019/bdw_statis_month=03/bdw_statis_day=19/000000_0")

    orcData.show()

  }
}
