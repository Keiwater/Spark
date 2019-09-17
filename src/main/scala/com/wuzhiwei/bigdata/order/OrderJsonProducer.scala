package com.wuzhiwei.bigdata.order

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  * 模拟产生订单数据Order
  */
object OrderJsonProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    props.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST)
    props.put("producer.type", ConstantUtils.PRODUCER_TYPE)
    props.put("serializer.class", ConstantUtils.SERIALIZER_CLASS)
    props.put("key.serializer.class", ConstantUtils.SERIALIZER_CLASS)

    var producer: Producer[String, String] = null

    try{

      val producerConfig = new ProducerConfig(props)

      producer = new Producer[String, String](producerConfig)

      // 新建容器：用于缓存消息
      val msgArrayBuffer = new ArrayBuffer[KeyedMessage[String, String]]()

      // TODO: 模拟一直产生订单数据，每次产生N条数据，此处使用while死循环模拟发送数据
      while(true){

        // 很关键一步： 清空缓存
        msgArrayBuffer.clear()

        val mapper : ObjectMapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        // 每次随机产生多少条订单数据，发送到Topic 中

        val randomNum = RandomUtils.getRandomNum(200000) + 3

        for(index : Int <- 0 until randomNum){

          val orderItem:SaleOrder ={

            //(orderId: String, provinceId: Int, orderPrice: Float)
            val orderID = UUID.randomUUID().toString
            val provinceID = RandomUtils.getRandomNum(34) + 1
            val orderPrice = RandomUtils.getRandomNum(80) + 0.5f

            SaleOrder(orderID, provinceID, orderPrice)

          }

          val orderJson: String = mapper.writeValueAsString(orderItem)

          println(s"Order Json ${orderJson}")
          println(s"Parse Json ${mapper.readValue(orderJson, classOf[SaleOrder])}")

          val msg: KeyedMessage[String, String] = new KeyedMessage(ConstantUtils.ORDER_TOPIC, orderItem.orderId, orderJson)

          msgArrayBuffer += msg

        }

        producer.send(msgArrayBuffer:_*)

        // 每次发送数据完成以后，稍微休息一下：
        Thread.sleep(RandomUtils.getRandomNum(100)*100)

      }

    }catch{
      case e: Exception => e.printStackTrace()

    }finally {
      if(producer != null){
        producer.close()
      }
    }

  }

}
