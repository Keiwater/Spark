package com.wuzhiwei.bigdata.scalaTest

object MatchCase {

  def main(args: Array[String]): Unit = {

    def fun(x:Any) = x match {

      case p => println("fun result is :"+p)}

    var p = "123456"

    fun("hello world")

  }

}