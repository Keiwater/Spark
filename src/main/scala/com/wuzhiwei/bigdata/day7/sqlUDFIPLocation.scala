package com.wuzhiwei.bigdata.day7

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object sqlUDFIPLocation {

  def ipToLong = (ip: String ) => {

    val fragments =  ip.split("[.]")
    var ipNum = 0L
    for( i <- 0 until fragments.length ){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
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
    val rulesDF: DataFrame = rulesDS.toDF("start_num","end_num","province")

    rulesDF.createTempView("v_rules")

    // 将规则变量，广播到app中去： -- 优化
    val broadcast: Broadcast[DataFrame] = spark.sparkContext.broadcast(rulesDF)
    // 读取访问日志
    val accessDS: Dataset[String] = spark.read.textFile(args(0))

    val ipDS: Dataset[String] = accessDS.map(line => {

      val fields = line.split("[|]")
      val ip = fields(1)
      ip

    })

    val ipDF: DataFrame = ipDS.toDF("ip")

    ipDF.createTempView("v_ip")

    // 注册 spark UDF
    spark.udf.register("ip2Long", ipToLong)

    val res: DataFrame = spark.sql("select province , count(*) from v_ip join v_rules on ( ip2Long(ip)>=  and ip2Long(ip)<= )")

    res.show()








    

  }
}
