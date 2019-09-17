package com.wuzhiwei.bigdata.SparkSql

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object DataSetWC {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("DataSetWC").master("local[*]").getOrCreate()

    val lines: Dataset[String] = session.read.textFile(args(0))

    // 导出session 对象中的隐式转换
    import session.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    words.show()

    val sortBycounts: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").sort($"counts" desc)

    sortBycounts.show()

  }
}
