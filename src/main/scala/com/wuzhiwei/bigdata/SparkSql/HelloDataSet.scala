package com.wuzhiwei.bigdata.SparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HelloDataSet {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("HelloDataSet")
      .master("local[*]")
      .getOrCreate()

//    session.sparkContext.textFile(args[])

    val lines: RDD[String] = session.sparkContext.parallelize(List("2,laoduan,20,99", "3,laoyang,20,100", "4,laozhang,21,100", "1,laozhao,18,9999"))

    val rowRDD: RDD[Row] = lines.map(line => {

      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      Row(id, name, age, fv)

    })

    val schema = StructType(
      List(
        StructField("id", LongType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("fv", IntegerType, true)

      )
    )


    val pdf: DataFrame = session.createDataFrame(rowRDD, schema)

    pdf.createTempView("v_person")

    val res: DataFrame = session.sql("select * from v_person  where id > 1 order by fv desc")

    res.show()

    session.close()


  }
}
