package com.wuzhiwei.bigdata.day5

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {


  //二分查找
  def binarySearch(lines: Array[(Long, Long, String)],ip:Long): Int ={

    var low = 0
    var high = lines.length-1
    while(low <= high){

      var middle = ( low + high ) /2

      //返回值处理
      if( ip >= lines(middle)._1 && ip <=lines(middle)._2){
        return middle
      }

      //二分位置处理
      if( ip <= lines(middle)._1){
        high = middle -1
      }else{
        low = middle +1
      }

    }
    -1
  }


  def ip2Long(ip: String): Long ={

    val fragments = ip.split("[|]")
    var ipNum = 0L
    for( i <- 0 until fragments.length ){

      ipNum = fragments(i).toLong | ipNum << 8L

    }
    ipNum

  }


  def data2MySQL(part: Iterator[(String, Int)]): Unit ={

    // 创建JDBC 链接：

    val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8","root","123")
    val statement: PreparedStatement = connection.prepareStatement("INSERT INTO access_log(province, count) values( ?, ?)")
    // 写入数据
    part.foreach( data =>{

      statement.setString(1,data._1)
      statement.setInt( 2,data._2)
      statement.executeUpdate()
    })

    statement.close()
    connection.close()

  }


  def main(args: Array[String]): Unit = {

    val ip = "121.69.54.202"
    val ipNum = ip2Long(ip)
    println(ipNum)


    //获取基础配置,指定从哪里 获取原始数据
    val conf: SparkConf = new SparkConf().setAppName("IPLocation").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


    //ip规则
    val lines: RDD[String] = sc.textFile(args(0))
    val rules: RDD[(Long, Long, String)] = lines.map(line => {

      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)


    })
    val ipRules: Array[(Long, Long, String)] = rules.collect()


    //广播 ip 规则
    //val broadcast: Broadcast[RDD[(Long, Long, String)]] = sc.broadcast(ipRules)
    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)


    //读取访问日志
    val accessLog: RDD[String] = sc.textFile(args(1))


    //整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLog.map(line => {

      val fields: Array[String] = line.split("[|]")
      val ip: String = fields(1)
      val ipNum: Long = ip2Long(ip)
      val arrayIndex: Int = binarySearch(broadcast.value, ipNum)
      //broadcast.value(arrayIndex)
      val province: String = broadcast.value(arrayIndex)._3
      (province, 1)

    })
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)


    //方案一：结果数据输出，在 Driver 端打印结果
    println(reduced.collect().toBuffer)

    //方案二：结果数据输出，将结果数据保存到数据库中

    reduced.foreachPartition( data =>{

      data2MySQL(data)

    })








    sc.stop()

  }

}
