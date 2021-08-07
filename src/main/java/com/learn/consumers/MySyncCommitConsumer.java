package com.learn.consumers;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MySyncCommitConsumer {
    private static KafkaConsumer<String, String> consumer;
    private static Properties properties = new Properties();

    static {


        // 这里不填写所有的broker也行，因为会从指定的broker读取到其他的broker信息
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //反序列化key值和value值
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "LearnKafkaSync");
    }

    private static void generalConsumeMessageSyncCommit() {
       properties.put("auto.commit.offset",false);
       consumer = new KafkaConsumer<String, String>(properties);
       consumer.subscribe(Collections.singletonList("learn_kafka_test_n"));

       while (true){
           boolean flag = true;
           ConsumerRecords<String ,String> records = consumer.poll(100);
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

           //这里每次for循环都要手动提交

           try {
               //这里会阻塞,失败会重试
               consumer.commitSync();
           }
           catch (CommitFailedException ex){
               System.out.println("commit fail error:" + ex.getMessage());
           }

           if (!flag){
               break;
           }
       }
    }

    public static void main(String[] args) {
        generalConsumeMessageSyncCommit();
    }
}
