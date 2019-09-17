package com.wuzhiwei.bigdata.day5

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Person(id:Long, name: String, age :Int, fv: Int)

object FaceValueTopNv2 {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf() 并设置App名称
    val conf = new SparkConf().setAppName("FaceValueTopN").setMaster("local[*]")

    val sc = new SparkContext(conf)
    // 创建sqlContext对象
    val sqlContext = new SQLContext(sc)


    val lines = sc.parallelize(List("2,laoduan,20,99", "3,laoyang,20,100", "4,laozhang,21,100", "1,laozhao,18,9999"))

    val userRDD = lines.map(_.split("[,]")).map( arr => Person(arr(0).toLong, arr(1), arr(2).toInt, arr(3).toInt) )


    /***
      *
      * 这里有一个  隐式转换的知识点： 要想使用 DataFrame 的toDF 方法， 需要使用到   import sqlContext.implicits._，
      * 但是这里的隐式转换是 源自于 sqlContext 类的，这里需要预先初始化一个 sqlContext 对象
      *
      *
      */
    import sqlContext.implicits._
    val personDF: DataFrame = userRDD.toDF

    //  将 DataFrame 注册成  临时表
    personDF.registerTempTable("t_person")


    //  执行，sql 方法，是一个transformation，不会执行
    val sqlRes: DataFrame = sqlContext.sql("select name, age , fv from t_person order by fv desc, age desc ")

    // 触发 action
    sqlRes.show()

    // 释放资源
    sc.stop()

  }
}
