package com.wuzhiwei.bigdata.Streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object kafkaDirectStream03 {


  val updateFunc = (it:Iterator[(String, Seq[Int], Option[Int])]) =>{

    it.map( t =>(t._1, t._2.sum + t._3.getOrElse(0)))

  }


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.WARN)


    // 消费所属组
    val group = "g10010"

    //    创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("kafkaDirectStream03").setMaster("local[*]")

    //    往sparkconf中设置参数，maxRatePerPatition 是指kafka 消费数据的 速度
    //    conf.set("spark.streaming.kafka.maxRatePerPartition","10000")

    // 创建SparkStreaming，并设置间隔时间
    val ssc: StreamingContext = new StreamingContext(conf, Duration(5))

    // topics
    val topic = "test"
    val brokerList = "localhost:9092"

    //指定 zk地址，后期更新消费的偏移量的时候使用
    val zkQuorum = "localhost:2181"
    val topics: Set[String] = Set(topic)

    // 创建一个 ZKGroupTopicDirs 对象，其实是指定往zk中写入数据的目录，用于 保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    // 获取 zookeeper 中的路径：
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      //      "metadata.broker.list"->brokerlist,
      "metadata.broker.list"->brokerList,
      //      "group.id"-> group,
      "group.id"-> group,
      // 位移： 偏移量
      "auto.offset.reset"-> kafka.api.OffsetRequest.SmallestTimeString
    )

    // zookeeper 的host  和ip ，创建一个 client
    val zkClient = new ZkClient(zkQuorum)

    // 查询 该路径下是否 有子节点，（默认有 子节点为 我们自己 保存不同partition 时生成的）,表示有多少个分区
    val children  = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    // 或者用这种写法
    //    val children  = zkClient.countChildren(zkTopicPath)

    var kafkaStream :InputDStream[(String, String)] = null
    // 如果 zookeeper 中保存有 offset 我们用 offset作为kafkaStream 的起始位置
    var fromOffsets : Map[TopicAndPartition, Long] = Map()

    //  如果 保存过 offset，这里更好的做法，还应该  和 kafka 上面 的最小的offset 做对比，不然会报OutOfRange
    if( children >0 ){

      for(i <- 0 until children){
        // 读zk，取偏移量, 我们 在后面的代码中，会把 对应的 offset 值维护到这个目标路径下面，这里用 readData 拿到对应的offset 值
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // acess-log ,0
        val tp = TopicAndPartition(topic , i)
        // 将不同 partition 对应的offset增加 到 fromOffsets 中
        // acess-log , 0 -> 1000
        // ********返回  一个Map 指定  topic 下面的 Partition 下面的 offset
        // var fromOffsets : Map[TopicAndPartition, Long] = Map()
        // 备注： topic 对应的分区下面的  offset
        fromOffsets += ( tp -> partitionOffset.toLong)

      }

      // 这个函数会 将kafka 的消息进行 transform， 最终 kafka 的数据都会变成 ( topic_name ,message ) 这样的
      val messageHandler = (mmd: MessageAndMetadata[String, String])=> (mmd.topic, mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String, String ,StringDecoder, StringDecoder, (String , String)](ssc, kafkaParams, fromOffsets, messageHandler )

    }
    else{

      // 如果未保存，根据 kafkaParam  的配置使用最新的或者最旧 的offset
      kafkaStream = KafkaUtils.createDirectStream[String ,String, StringDecoder, StringDecoder]( ssc,kafkaParams,topics )

    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform{ rdd => {

      // 得到 该 rdd 对应的 kafka 消息的offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
      }

    }.map( msg => msg._2)

      .foreachRDD{ rdd =>
        rdd.foreachPartition(
          partition => partition.foreach{
            println
          }
        )

        for( o <- offsetRanges ){
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          // 将 该partition 的offset 保存 到zookeeper
          ZkUtils.updatePersistentPath(zkClient,zkPath, o.fromOffset.toString)

        }
      }


    ssc.start()
    ssc.awaitTermination()


  }
}
