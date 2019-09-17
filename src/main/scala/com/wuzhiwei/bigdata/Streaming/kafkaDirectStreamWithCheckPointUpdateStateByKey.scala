package com.wuzhiwei.bigdata.Streaming

import kafka.serializer.StringDecoder
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaDirectStreamWithCheckPointUpdateStateByKey {


  val updateFunc = (it:Iterator[(String, Seq[Int], Option[Int])]) =>{

    it.map( t =>(t._1, t._2.sum + t._3.getOrElse(0)))

  }


  val CHECK_POINT_PATH = "E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\Streaming\\kafkaCK01"

  def main(args: Array[String]): Unit = {


    val context: StreamingContext = StreamingContext.getOrCreate(CHECK_POINT_PATH,
      // creatingFunc: () => StreamingContext,
      () => {
        val conf: SparkConf = new SparkConf().setAppName("kafkaDirectStream").setMaster("local[*]")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        ssc.sparkContext.setLogLevel("WARN")

        ssc.checkpoint(CHECK_POINT_PATH)

        DataProcess(ssc)

        ssc

      }
    )

    context.start()
    context.awaitTermination()
    context.stop(stopSparkContext = true, stopGracefully = true)


  }

  private def DataProcess(ssc: StreamingContext): Unit = {
    val kafkaParams = Map(

      //      "metadata.broker.list"->brokerlist,
      "metadata.broker.list" -> "localhost:9092",
      //      "group.id"-> group,
      "group.id" -> "123",

      // 位移： 偏移量
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString

    )

    val topics = Set("test")

    val directStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //    directStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.checkpoint(CHECK_POINT_PATH)
    //
    //    // 可更新值的方式
    directStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), rememberPartitioner = true).print()
  }
}
