package com.wuzhiwei.bigdata.Streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object kafkaDirectStreamWithCheckPointMapWithState {


  /**
    *
    * 有变化才更新：使用mapWithState实现；没有状态信息更新的时候，不用返回数据信息；优化了性能
    * 建议： 针对使用mapWithState 的时候，建议先聚合，再 更新状态； 在mapWithState 之前，+ reduceByKey(_+_)
    *
    */

  val updateFunc = (it:Iterator[(String, Seq[Int], Option[Int])]) =>{

    it.map( t =>(t._1, t._2.sum + t._3.getOrElse(0)))

  }

  private def DataProcess(ssc: StreamingContext): Unit = {
    val kafkaParams = Map(

      //      "metadata.broker.list"->brokerlist,
      "metadata.broker.list" -> "localhost:9092",
      //      "group.id"-> group,
      "group.id" -> "1234",

      // 位移： 偏移量
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString

    )

    val topics = Set("test")

    val directStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //    directStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    // 定义数据更新的方法
    val mappingFunction =
      (word:String, count: Option[Int], state : State[Int]) => {

        // 1.获取当前记录中的状态值：
        val previousState: Int = state.getOption().getOrElse(0)

        // 2.获取当前捕获到的数据对应的状态值：
        val lastestState: Int = count.getOrElse(0) + previousState

        // 3.更新状态值中 与word对应的  state 变量
        state.update(lastestState)

        // 4.返回最新的DStream kv对儿
        (word,lastestState)

      }

    val WordCountTotalDstream: MapWithStateDStream[String, Int, Int, (String, Int)] = directStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).mapWithState(

      // [KeyType, ValueType, StateType, MappedType] 函数泛型 类型定义
      StateSpec.function[String, Int, Int, (String, Int)](mappingFunction)

    )
    // 结果处理：将分析的结果打印在控制台上：
    WordCountTotalDstream.foreachRDD((rdd,time) => {

      println("-------------------------------------------")
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))}")
      println("-------------------------------------------")
      if(!rdd.isEmpty()){
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))

      }

      println()


    })
  }

  val CHECK_POINT_PATH :String = "E:\\WorkSpaces\\Spark\\src\\main\\scala\\com\\wuzhiwei\\Streaming\\kafkaCK03"


  def main(args: Array[String]): Unit = {


    val context: StreamingContext = StreamingContext.getOrCreate(CHECK_POINT_PATH,
      // creatingFunc: () => StreamingContext,
      () => {
        val conf: SparkConf = new SparkConf().setAppName("kafkaDirectStreamMapWithState").setMaster("local[*]")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        ssc.sparkContext.setLogLevel("WARN")

        ssc.checkpoint(CHECK_POINT_PATH)

        DataProcess(ssc)

        ssc

      }
    )

    context.sparkContext.setLogLevel("WARN")
    context.start()
    context.awaitTermination()
    context.stop(stopSparkContext = true , stopGracefully = true)

  }


}
