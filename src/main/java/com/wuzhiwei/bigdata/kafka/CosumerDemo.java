package com.wuzhiwei.bigdata.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import org.apache.kafka.clients.consumer.ConsumerConfig;

public class CosumerDemo {

    private static final String topic = "dataTest";
    private static final Integer threads = 2;

    public static void main(String[] args) {

        Properties props = new Properties();
        // consumer需要指定链接ZK 的URl
        props.put("zookeeper.connect","localhost:2181");
        // 指定一个 组ID
        props.put("group.id","123");
        // 指定 数据 从何处开始读取：smallest 代表从数据的最开始 读取
        props.put("auto.offset.reset","smallest");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic,threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);


        for( KafkaStream<byte[], byte[]> tmpStream : kafkaStreams) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> mm : tmpStream) {
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }
            }).start();


            System.out.println("+++++++++++++");

        }

    }
}
