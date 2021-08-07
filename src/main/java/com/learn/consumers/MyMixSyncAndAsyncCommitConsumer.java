package com.learn.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyMixSyncAndAsyncCommitConsumer {
    private static KafkaConsumer<String, String> consumer;
    private static Properties properties = new Properties();

    static {


        // 这里不填写所有的broker也行，因为会从指定的broker读取到其他的broker信息
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //反序列化key值和value值
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "LearnKafkaMix");
    }


    //混合同步和异步的方式
    private static void generalConsumeMessageSyncAndAsyncCommit() {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("learn_kafka_test_n"));

        try{
            while (true) {
                boolean flag = true;
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
                    if (!flag) {
                        break;
                    }

                }
                //先异步一波
                consumer.commitAsync();
            }
        }
        catch (Exception ex) {
            System.out.println("commit async error: "+ex.getMessage());
        } finally {
            try {
                //异步失败了同步一波
                consumer.commitSync();
            }
            finally {
                //最后关闭消费者
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        generalConsumeMessageSyncAndAsyncCommit();
    }
}
