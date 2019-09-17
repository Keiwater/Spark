package com.wuzhiwei.bigdata.Streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // 创建 一个SparkStremingContext

    val conf: SparkConf = new SparkConf().setAppName("hexin.bigdata.export.util").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val streamsc = new StreamingContext(sc,Seconds(5))

//    172.20.11.180
    val lines: ReceiverInputDStream[String] = streamsc.socketTextStream("172.20.11.180",8888)

    val words: DStream[String] = lines.flatMap(_.split("[ ]"))

    val reduced: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)

    // 结果处理
    reduced.print()

    // 需要调用启动 start 来调用  sparkStream
    streamsc.start()
    streamsc.awaitTermination()








  }

}
