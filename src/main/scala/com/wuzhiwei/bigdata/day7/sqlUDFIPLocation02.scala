package com.wuzhiwei.bigdata.day7

/**
  *
  *  优化：
  *
  *  将中间数据集结果：存parquet；
  *  中间结果集：Driver端拿到后，规则小表，维度表 进行广播，加载入各个 Executor 端的内存中，类MapJoin的思路
  *  定义SparkUDF，
  *
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object sqlUDFIPLocation02 {


  def ipToLong = (ip: String ) => {

    val fragments =  ip.split("[.]")
    var ipNum = 0L
    for( i <- 0 until fragments.length ){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  // 查找的前提是排序  二分查找
  def binarySearch( lines : Array[(Long,Long,String)], ip: Long ): Int ={

    var low = 0
    var high = lines.length - 1
    while ( low <= high ){

      val middle = (low + high) /2

      if((ip >= lines(middle)._1) && ( ip <=lines(middle)._2)){
        return middle
      }
      else{

        if( ip < lines(middle)._1 ){
          high = middle -1
        }else{
          low = middle +1
        }
      }

    }

    -1
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sqlUDFIPLocation").master("local[*]").getOrCreate()
    val ruleLines: Dataset[String] = spark.read.textFile("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\day7\\visit")

    import spark.implicits._


    // 5是ip，2就当做startNum，3就当做是endNum,6是国家，7是省份，8是城市
    val rulesDS: Dataset[(Long, Long, String)] = ruleLines.map(line => {
      val fields = line.split("[,]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(7)
      (startNum, endNum, province)
    })


    // 优化步骤
    val rules: Array[(Long, Long, String)] = rulesDS.collect()
    // 将规则变量，广播到app中去： -- 优化
    val broadcast: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rules)
    // 这样的设计逻辑，避免了，两表数据集合在执行 SparkSql的时候的 Join， 产生大量的shuffle


    // 读取访问日志,已经被 规范化压缩存储到了 parquet 文件当中去了
    val accessLogDF: DataFrame = spark.read.parquet(args(0))
    // DF注册视图
    accessLogDF.createTempView("v_access")


    // 注册Spark  UDF
    spark.udf.register("ip2Province",( ip: String )=>{

      val ipNum: Long = ipToLong(ip)
      val rulesLine : Array[(Long, Long, String)] = broadcast.value
      val ipIndex: Int = binarySearch( rulesLine ,ipNum)

      rulesLine(ipIndex)._3

    })



    //  使用 注册好的 Spark UDF 执行SQL
    spark.sql("select ip2Province(ip) , count(*) from v_access group by ip2Province(ip) ").show()

  }
}
