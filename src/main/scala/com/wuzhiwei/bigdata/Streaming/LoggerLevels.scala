package com.wuzhiwei.bigdata.Streaming

/***
  * 设置 运行Logger 级别
  *
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

//import org.apache.spark.internal.Logging
//import org.spark_project.jetty.util.log.Logger



object LoggerLevels extends Logging{

  def setStreamingLogLevels(): Unit ={
    val log4jInitialized: Boolean = Logger.getRootLogger().getAllAppenders.hasMoreElements
    if(!log4jInitialized){
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

}
