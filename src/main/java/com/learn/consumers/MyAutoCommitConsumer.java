package com.learn.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyAutoCommitConsumer {
    private static KafkaConsumer<String, String> consumer;
    private static Properties properties = new Properties();

    static {


        // 这里不填写所有的broker也行，因为会从指定的broker读取到其他的broker信息
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //反序列化key值和value值
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "LearnKafkaAutoCommit");
    }

    private static void generalConsumeMessageAutoCommit() {
        //自动commit
        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("learn_kafka_test_n"));

        try {
            while (true) {
                boolean flag = true;

                //poll是不停拉取topic消息操作
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, value =%s",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    ));

                    //如果消息内容为done的话就跳出死循环
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }

                if (!flag) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        generalConsumeMessageAutoCommit();
    }
}
