package com.wuzhiwei.bigdata.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object MapSideJoin {

  /***
    *
    * 模拟 MR 的 map side join ，有一个小文件，这个小文件要跟一个大文件  进行JOIN,如果在默认情况下会产生
    * 可以先  将 小文件缓存  到Map 端，其实就是先将小文件的数据 加载 到内存当中
    *
    *
    */

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("MapSideJoin").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)


    /***
      * 这里创建了 一个rules，如果在Driver端创建的一个变量，并且在传入到RDD方法的函数当中，
      * RDD 方法中传入的函数的执行是在  Executor 的Task 中，则 Driver 端需要 把 Rules 定义的mapjoin 规则，发送到每一个Task 当中去 ，
      * 因此在执行过程中，会有大量的网络传输，执行时间会延长
      *
      */
    val rules = Map("cn" -> "中国","us"->"美国","jp"->"日本")

    val lines = sc.textFile(args(0))

    val res: RDD[(String, String)] = lines.map(line => {

      val filed = line.split(",")
      val stuName: String = filed(0)
      val nation_code: String = filed(1)
      val nation_name: String = rules(nation_code)
      (stuName, nation_name)
    })

    println(res.collect().toBuffer)

    sc.stop()

  }
}