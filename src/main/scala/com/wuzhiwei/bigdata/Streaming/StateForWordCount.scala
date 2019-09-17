package com.wuzhiwei.bigdata.Streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 可更新的 sparkStream
  *
  */

object StateForWordCount {

  /***
    * 参数注释：
    * 第一个：Key 即需要统计的单词
    * 第二个：当前批次该单词出现的次数
    * 第三个：初始值，或者是以前累加过的值
     */
  val updateFunc =( it: Iterator[(String, Seq[Int], Option[Int])] ) => {

    it.map( t=> (t._1 , t._2.sum + t._3.getOrElse(0)))

    //  另外一种写法：
    it.map{ case( w, s, o ) => (w, s.sum + o.getOrElse(0) )}

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("StateForWordCount").setMaster("local[*]")

//    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(conf, Milliseconds(5000))

    ssc.checkpoint("E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\Streaming\\ck")

    ssc.sparkContext.setLogLevel("WARN")

//    StreamingContext.getOrCreate()

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("172.16.10.192",8888)

    val keyAndOne: DStream[(String, Int)] = lines.flatMap(_.split("[ ]")).map((_,1))

    val reduced: DStream[(String, Int)] = keyAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultMinPartitions) ,true )

    // 结果处理
    reduced.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
