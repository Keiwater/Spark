package com.wuzhiwei.bigdata.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FaceValueTopN {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FaceValueTopN").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("2,laoduan,20,99", "3,laoyang,20,100", "4,laozhang,21,100", "1,laozhao,18,9999" ))

    val userRDD: RDD[(Long, String, Int, Int)] = lines.map(line => {

      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val faceValue = fields(3).toInt
      (id, name, age, faceValue)

    })
//    userRDD

    val sorted: RDD[(Long, String, Int, Int)] = userRDD.sortBy( t => User(t._1, t._2,t._3, t._4))

    print(sorted.take(3).toBuffer)
  }
}


// 声明样例类：

case  class User (id :Long, name: String, age :Int, fv: Int) extends Comparable[User] {
  override def compareTo(that: User): Int = {

    if( this.fv == that.fv ){

      this.age - that.age

    }else{
      that.fv - this.fv
    }

  }
}