package com.wuzhiwei.bigdata.day5

import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 将数据库当中的  数据，使用RDD 进行计算
  *
  */

object JDBCRddDemo {

//   //定义  数据库链接变量
//  val getMysqlConnection: sql.Connection = {
//    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8","root","123")
//  }


  val getMysqlConnection = ()=> {
    DriverManager.getConnection("jdbc:mysql://172.20.10.39:3306/hexindai_dis?characterEncoding=UTF-8","dev_select","OdQ71bFMkxtfKi5HYz")
  }



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JDBCRddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jdbcRdd = new JdbcRDD(
      sc,
      getMysqlConnection,
      "select * from t07_county where county_id >=? and county_id < ?  ",
      1,
      5,
      2,
      rs => {
        val county_id = rs.getInt("county_id")
        val city_id = rs.getInt("city_id")
        val county_name = rs.getString("county_name")
        (county_id, city_id, county_name)

      }

    )

    val r = jdbcRdd.collect().toBuffer

    println(r)

  }
}
