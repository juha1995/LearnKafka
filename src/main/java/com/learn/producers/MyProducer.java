package com.learn.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    private static KafkaProducer<String, String> producer;

    static {
        Properties properties = new Properties();
        // 这里不填写所有的broker也行，因为会从指定的broker读取到其他的broker信息
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //序列化key值和value值
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //把自定义的消息分区器加进来
        properties.put("partitioner.class", "com.learn.CustomPartitioner");

        producer = new KafkaProducer<>(properties);

    }

    //一种简单的发送消息机制
    private static void sendMessageForgerResult() {
        ProducerRecord producerRecord = new ProducerRecord<String, String>("learn_kafka_test", "yes it is");
        producer.send(producerRecord);
        producer.close();
    }


    //同步发送
    private static void senMessageSync() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("learn_kafka_test", "name", "sync");

        RecordMetadata result = producer.send(record).get();

        System.out.println(result.topic());
        System.out.println(result.partition());
        //这条记录的偏移量
        System.out.println(result.offset());

    }

    //异步发送的回调类
    private static class MyProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
                return;
            }

            System.out.println("topic名称为" + recordMetadata.topic());
            System.out.println("分区为" + recordMetadata.partition());
            System.out.println("消息偏移量为" + recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");

        }
    }

    private static void sendMessageCallback() {
//        ProducerRecord record = new ProducerRecord<String, String>("learn_kafka_test", "name", "callback");
//        producer.send(record, new MyProducerCallBack());
//
        int i = 10;
        while ( i >0){
            ProducerRecord record1 = new ProducerRecord<String, String>("learn_kafka_test_n", "name", "callback");
            producer.send(record1, new MyProducerCallBack());
            ProducerRecord record2 = new ProducerRecord<String, String>("learn_kafka_test_n", "name-x", "callback");
            producer.send(record2, new MyProducerCallBack());
            ProducerRecord record3 = new ProducerRecord<String, String>("learn_kafka_test_n", "name-x", "callback");
            producer.send(record3, new MyProducerCallBack());
            ProducerRecord record4 = new ProducerRecord<String, String>("learn_kafka_test_n", "name-y", "callback");
            producer.send(record4, new MyProducerCallBack());
            ProducerRecord record5 = new ProducerRecord<String, String>("learn_kafka_test_n", "name-y", "callback");
            producer.send(record5, new MyProducerCallBack());
            i--;
        }

        producer.close();

    }
    //异步发送,生产环境常用，但是比第一种慢一点


    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        sendMessageForgerResult();
//        senMessageSync();
        sendMessageCallback();
    }

}
