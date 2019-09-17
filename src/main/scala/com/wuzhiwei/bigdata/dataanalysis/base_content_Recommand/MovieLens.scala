package com.wuzhiwei.bigdata.dataanalysis.base_content_Recommand

import org.apache.spark.sql.SparkSession

object MovieLens {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession
      .builder()
      .appName("base-content-Recommand") //spark任务名称
      .enableHiveSupport()
      .getOrCreate()



    //从hive中，获取rating评分数据集，最终形成如下格式数据(movie,avg_rate)
    val movieAvgRate = sparkSession.sql("select movieid,round(avg(rate),1) as avg_rate  from tx.tx_ratings group by movieid").rdd.map{
      f=>
        (f.get(0),f.get(1))
    }
    //获取电影的基本属性数据，包括电影id，名称，以及genre类别
    val moviesData = sparkSession.sql("select movieid,title,genre from tx.tx_movies").rdd
    //获取电影tags数据，这里取到所有的电影tag
    val tagsData = sparkSession.sql("select movieid,tag from tx.tx_tags").rdd




  }
}
