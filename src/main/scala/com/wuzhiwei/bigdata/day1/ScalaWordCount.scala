package com.wuzhiwei.bigdata.day1

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf 并且 设置配置参数
    val conf = new SparkConf().setAppName("scalaWordsCount").setMaster("local")

    // sc 是SparkContext ，他是spark 程序执行的入口
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))

    sc.stop()


  }
}
