package com.wuzhiwei.bigdata.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // props 用户保存配置信息
        Properties props = new Properties();
        // 添加配置信息： metadata.broker.list 指定的Kafka的Broker的地址和端口，可以是多个  Broker的地址
        props.put("metadata.broker.list","localhost:9092");
        // 数据写入到Kafka中使用的是 序列化的方式
        props.put("serializer.class","kafka.serializer.StringEncoder");
        // 通过，props 创建一个ProducerConfig
        ProducerConfig producerConfig = new ProducerConfig(props);

        // 创建一个 Producer
//        Producer<String, String> producer = new Producer<>(producerConfig);

        Producer<String, String> producer = new Producer<>(producerConfig);

        for(int i=1001;i <=2000; i++){

            producer.send(new KeyedMessage<String,String>("dataTest","hello " +i));
        }

    }
}
