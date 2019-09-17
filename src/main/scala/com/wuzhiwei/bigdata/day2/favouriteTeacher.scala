package com.wuzhiwei.bigdata.day2

/***
  * 找出学科内部，受欢迎老师的前两名
  *
  *
  */


import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object favouriteTeacher {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("favouriteTeacher").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectAndTeacher: RDD[(Any, String)] = lines.map(line => {

      val url = new URL(line)
      val host: String = url.getHost
      val subject: Any = host.substring(0, host.indexOf("."))
      val teacher: String = url.getPath.substring(1)
      (subject, teacher)

    })

    val reduceRes: RDD[((Any, String), Int)] = subjectAndTeacher.map((_,1)).reduceByKey(_+_)

    val groupedRes: RDD[(Any, Iterable[((Any, String), Int)])] = reduceRes.groupBy(_._1._1)

    val sortedRes: RDD[(Any, List[((Any, String), Int)])] = groupedRes.mapValues(_.toList.sortBy(_._2).reverse)

    val arrRes = sortedRes.collect()

    print(arrRes.toBuffer)

    sc.stop()


  }
}


