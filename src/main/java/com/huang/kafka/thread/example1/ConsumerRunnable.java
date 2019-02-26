package com.huang.kafka.thread.example1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * @author 黄世增
 */

public class ConsumerRunnable implements Runnable {

    //每个线程维护私有的KafkaConsumer实例
    private final KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        //键序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //值序列化
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
        this.topic = topic;

    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                //1000是等待超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(Thread.currentThread().getName());
                    System.out.println("receive: key = " + record.key() + ", value = " + record.value() + ", offset = " + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
