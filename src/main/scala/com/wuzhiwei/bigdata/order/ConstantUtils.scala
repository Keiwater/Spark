package com.wuzhiwei.bigdata.order

/**
  * 存储一些常量数据
  */
object ConstantUtils {

  /**
    * 定义KAFKA相关集群配置信息
    */
  //  KAFKA CLUSTER BROKERS
  //  val METADATA_BROKER_LIST = "bigdata-training01.wuzhiwei.com:9092,bigdata-training01.wuzhiwei.com:9093,bigdata-training01.wuzhiwei.com:9094"
  val METADATA_BROKER_LIST = "localhost:9092"

  // OFFSET
  val AUTO_OFFSET_RESET = "largest"

  // 序列化类
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"

  // 发送数据方式
  val PRODUCER_TYPE = "async"

  // Topic的名称
  val ORDER_TOPIC = "saleOrderTopic"
}

