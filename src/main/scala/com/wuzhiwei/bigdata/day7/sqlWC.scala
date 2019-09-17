package com.wuzhiwei.bigdata.day7

import java.sql.Connection

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object sqlWC {

  def dataToMysql(rows: Iterator[Row]): Unit ={
    // 创建JDBC链接
    val conn: Connection = null

    rows.foreach( r => {
      val w = r.getAs[String]("word")
      val c = r.getAs[Int]("counts")

    })

  }


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("DataSetWC").master("local[*]").getOrCreate()

    val lines: Dataset[String] = session.read.textFile(args(0))

    // 导出session 对象中的隐式转换
    import session.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    val df: DataFrame = words.withColumnRenamed("value","word")

    df.createTempView("v_wc")

    val res: DataFrame = session.sql("select word, count(*) counts from v_wc group by word order by counts desc")

    res.show()

//    res.foreachPartition( part => {
//
//      dataToMysql(part)
//    })
//    res.write.mode("append").parquet("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\parquet")

    res.write.mode("append").orc("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\orc")
  }
}
