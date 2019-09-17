package com.wuzhiwei.bigdata.Streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object OnlineUserInfo {




  /**
    * Created by lq on 2017/8/29. 使用的是 object  不是class 实现单例：
    */
  object JedisConnectionPool {

    val config = new JedisPoolConfig()
    //最大连接数
    config.setMaxTotal(10)
    //最大空闲连接数
    config.setMaxIdle(5)
    //当调用borrow object 方法时,是否进行有效性验证
    config.setTestOnBorrow(true)
    val pool = new JedisPool(config, "localhost", 6379)

    def getConnection(): Jedis = {
      pool.getResource
    }
  }

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


  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {

    it.map( line => (line._1, line._2.sum + line._3.getOrElse(0)))

  }


  /***
    *
    * // 读取：IP地址映射 规则 并且广播变量：
    * val ruleData = sc.textFile(args(0))
    * // 指定组名称：
    * val group = args(1)
    * ssc.checkpoint(args(2))
    *
    * /bigdata/spark-2.1.1-bin-hadoop2.6/bin/spark-submit --master spark://node-1.xiaoniu.com:7077
    * --executor-memory 1g --total-executor-cores 4
    * --class com.xiaoniu.spark.streaming.OnlineUserInfo /root/Streaming-1.0-SNAPSHOT.jar
    * // 传入main参数：
    * hdfs://node-1.xiaoniu.com:9000/ip.txt g2222 hdfs://node-1.xiaoniu.com:9000/ck517  gameOnline
    *
    *
    * /bigdata/kafka_2.11-0.8.2.1/bin/kafka-console-producer.sh --broker-list node-1.xiaoniu.com:9092 node-2.xiaoniu.com:9092 node-3.xiaoniu.com:9092 --topic gameonline
    *
    *
    * @param args
    */

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.WARN)


    // 创建SparkConf
    val conf = new SparkConf().setAppName("OnlineUserInfo").setMaster("local[2]")
    val sc = new SparkContext(conf)


    // 读取：IP地址映射 规则 并且广播变量：
    val ruleData = sc.textFile(args(0))
    // 指定组名称：
    val group = args(1)

    // 整理规则数据：
    val rulesRDD : RDD[(Long, Long, String)] = ruleData.map(line => {
      val fields: Array[String] = line.split("\\|")
      val starNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (starNum, endNum, province)

    })

    // 收集 数据 大Driver 段
    val allRules: Array[(Long, Long, String)] = rulesRDD.collect()
    // 广播 规则：
    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(allRules)


    // 设置 SparkConf 参数：maxRatePerPartition 是指：kafka 消费数据的速度：
    // conf.set("spark.streaming.kafka.maxRatePerPartition","10000")
    conf.set("spark.streaming.backpressure.enable","true")

    // 创建 SparkStreaming 对象，并设置 时间间隔：
    val ssc: StreamingContext = new StreamingContext(sc, Duration(5000))

    // 指定 checkpoint
    ssc.checkpoint(args(2))


    // 指定消费的 topic
    val topic = "game_log"
    // 指定kafka broker
    val brokerList = "localhost:9092"
    // 指定Zk 地址：,用于 在数据处理的后期，存放 和更新 消费的偏移量时 使用
    val zkQuorum = "localhost:2181"
    // 创建一个 ZKGroupTopicDirs  对象， 指定往zk 中写入数据的路径： 用于保存 偏移量
    val zkGroupTopicDirs = new ZKGroupTopicDirs(group, topic) with Serializable
    // 获取ZK 中的路径：
    val zkTopicPath = s"${zkGroupTopicDirs.consumerOffsetDir}"

    // 准备初始化 kafkaStream对象，需要用到的参数：
    val topics : Set[String] = Set(topic)
    // 准备kafka 的参数：
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group ,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    // 创建 ZkCLient
    val zkClient: ZkClient = new ZkClient(zkQuorum) with Serializable
    // 查询该路径下面 是否存在子 节点，( 默认有 子节点为我们保存 不同的partition 时产生的 ) ,表示有多少个分区
    val children: Int = zkClient.countChildren(s"$zkTopicPath")


    var kafkaStream : InputDStream[(String, String)] = null
    // 如果 zk 中 保存有 offset ，我们会 利用 这个offset 作为 kafkaStream 的起始值 偏移量
    var fromOffsets: Map[TopicAndPartition, Long] = Map()


    // 植入 处理逻辑：
    // 如果存过 offset，这里更好的做法，还应该 和 kafka 上最小的 offset 做对比，不然会报OutOfRange
    if( children > 0 ){

      for( i <- 0 until children){
        // 读zk，取偏移量, 我们 在后面的代码中，会把 对应的 offset 值维护到这个目标路径下面，这里用 readData 拿到对应的offset 值
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${i}")
        val topicAndPartition = TopicAndPartition(topic,i)
        fromOffsets += (topicAndPartition -> partitionOffset.toLong)
      }

      // 这个函数会将 kafka 的消息进行 transform， 最终kafka 的数据都会变成 ( topic_name , message ) 这样的
      val messageHandler = ( mmd: MessageAndMetadata[String, String]) => ( mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String) ](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
      // 如果 没有保存，根据  kafkaParams 的配置 使用最新 或者最旧 的 offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }


    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
    kafkaStream.transform{ rdd =>
      // 这里使用了一下 transform 就是为了得到该rdd 对应 kafka 的消息 的offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map( topicAndMsg => {

      val msg = topicAndMsg._2
      val fields = msg.split(",")
      var flag = fields(2).toInt
      if( flag == 0 ) flag = -1

      // 计算 ip 归属地
      val ip = fields(3)
      val ipNum = ip2Long(ip)
      val ipRules: Array[(Long, Long, String)] = broadcast.value
      // 调用 二分查找 去index
      val index = binarySearch(ipRules,ipNum)
      val province: String = ipRules(index)._3
      (province, flag)

    } ).updateStateByKey(updateFunc , new HashPartitioner(ssc.sparkContext.defaultMinPartitions),rememberPartitioner = true)
    // 获取每一个 批次的 RDD 的数据
      .foreachRDD( rdd =>
      rdd.foreachPartition( partition => {

        // 更新 统计分析结果
        //  一个RDD分区（每一个 task ） 创建一个 jedis 链接
        val conn = JedisConnectionPool.getConnection()
        partition.foreach( t => {
          println(t)
          conn.set(t._1, t._2.toString)

        })

        conn.close()
        // 更新 zk 偏移量
        for( i <- offsetRanges ){
          val zkPath = s"${zkGroupTopicDirs.consumerOffsetDir}/${i.partition}"
          // 将 该partition 的offset 保存到 zookeeper
          ZkUtils.updatePersistentPath(zkClient, zkPath, i.fromOffset.toString)
        }

      } )
    )

    ssc.start()
    ssc.awaitTermination()


  }

}
