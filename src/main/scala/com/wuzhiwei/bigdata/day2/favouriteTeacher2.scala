package com.wuzhiwei.bigdata.day2

/***
  * 找出学科内部，受欢迎老师的前两名
  * 根据学科信息，进行自定义分区，而不是采用默认的  hashPartioner作为数据的分区器
  *
  */

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable


object favouriteTeacher2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("favouriteTeacher").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectAndTeacher: RDD[(String, String)] = lines.map(line => {

      val url = new URL(line)
      val host: String = url.getHost
      val subject: String = host.substring(0, host.indexOf("."))
      val teacher: String = url.getPath.substring(1)
      (subject, teacher)

    })


    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.map((_,1)).reduceByKey(_+_)

    // 将 reduced 这个RDD 后面要反复使用，将其RDD加入缓存

    reduced.cache()


    // 获取数据中的 学科信息

    val subjects = reduced.map(_._1._1.toString).distinct().collect()


    // 定义 分区器
    val subjectPartitioner = new SubjectPartitioner(subjects)
    // 安装自定义分区器的规则shuffle

    val partitions: RDD[(String, (String, Int))] = reduced.map(t => (t._1._1,(t._1._2,t._2))).partitionBy(subjectPartitioner)


    val res: RDD[(String, (String, Int))] = partitions.mapPartitions(_.toList.sortBy(_._2._2).reverse.take(2).iterator)

//    val collect: Array[Any] = subjects.collect()

    print(res.collect().toBuffer)

    sc.stop()



  }
}


class SubjectPartitioner(subjects: Array[String]) extends Partitioner {

  // 定义分区的规则
  val rules = new mutable.HashMap[String,Int]()
  var i : Int = 1
  for( sub <- subjects){

    rules += (sub -> i)
    i += 1
  }

  // 有多少个分区
  override def numPartitions: Int = subjects.length + 1

  override def getPartition(key: Any): Int = {

    val k = key.toString
    rules.getOrElse(k,0)

  }
}


