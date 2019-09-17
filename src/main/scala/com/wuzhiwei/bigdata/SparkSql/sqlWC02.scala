package com.wuzhiwei.bigdata.SparkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object sqlWC02 {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("DataSetWC").master("local[*]").getOrCreate()

//    val lines: Dataset[String] = session.read.textFile(args(0))
//
//    // 导出session 对象中的隐式转换
//    import session.implicits._
//
//    val words: Dataset[String] = lines.flatMap(_.split(" "))
//
//    val df: DataFrame = words.withColumnRenamed("value","word")
//
//    df.createTempView("v_wc")
//
//    val res: DataFrame = session.sql("select word, count(*) counts from v_wc group by word order by counts desc")
//
//    res.show()

    import session.implicits._
    val lines: DataFrame = session.read.format("text").load(args(0))

    val words: Dataset[String] = lines.flatMap(line => {

      val wordsArr: mutable.ArrayOps[String] = line.getAs[String]("value").split(" ")
      wordsArr

    })

//    words.show()

    val wordsDF: DataFrame = words.toDF("word")
    wordsDF.createTempView("v_wc")

    val rs: DataFrame = session.sql("select word ,count(*) as counts from v_wc group by word order by counts desc")

    rs.show()


  }
}
