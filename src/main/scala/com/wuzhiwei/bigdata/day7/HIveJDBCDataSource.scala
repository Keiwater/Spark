package com.wuzhiwei.bigdata.day7

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HIveJDBCDataSource {

  def dataToMysql(rows: Iterator[Row]): Unit ={
    // 创建JDBC链接
    val conn: Connection = null

    rows.foreach( r => {
      val w = r.getAs[String]("word")
      val c = r.getAs[Int]("counts")

    })

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JDBCDataSource").master("local[*]").getOrCreate()

    val inputData: DataFrame = spark.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://127.0.0.1:3306/test"
        ,"driver"->"com.mysql.jdbc.Driver"
        ,"dbtable"->"logs"
        ,"user"->"root"
        ,"password"->"123")
      ).load()

    inputData.show()

//    inputData.where(inputData.col("id")>=3).write.mode("append").parquet("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\parquet")

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123")
//    inputData.where(inputData.col("id")>=3).write.mode("append").format("jdbc").options(Map("url"->"jdbc:mysql://127.0.0.1:3306/test"
//      ,"driver"->"com.mysql.jdbc.Driver"
//      ,"dbtable"->"logs"
//      ,"user"->"root"
//      ,"password"->"123"))
    inputData.where(inputData.col("id")>=3).write.mode("append").jdbc("jdbc:mysql://127.0.0.1:3306/test","test.logs", prop)



  }
}
