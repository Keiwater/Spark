package com.wuzhiwei.bigdata.day5

object IPDemo {

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

    val fragments = ip.split("[.]")
    var ipNum = 0L
    for( i <- 0 until fragments.length ){

      ipNum = fragments(i).toLong | ipNum << 8L

    }
    ipNum

  }


  def main(args: Array[String]): Unit = {

    val ip = "121.69.54.202"
    val ipNum = ip2Long(ip)
    println(ipNum)

  }

}
