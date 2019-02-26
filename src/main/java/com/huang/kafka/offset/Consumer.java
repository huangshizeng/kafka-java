package com.huang.kafka.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author 黄世增
 */

public class Consumer {

    private final KafkaConsumer<String, String> consumer;
    private static final String topic = "my-topic";

    public Consumer() {
        Properties props = new Properties();
        //kafka的地址
        props.put("bootstrap.servers", "localhost:9092");
        //组名 不同组名可以重复消费。例如你先使用了组名A消费了kafka的1000条数据，但是你还想再次进行消费这1000条数据，
        //并且不想重新去产生，那么这里你只需要更改组名就可以重复消费了
        props.put("group.id", "test");
        //手动提交
        props.put("enable.auto.commit", "false");
        //键序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //值序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    public void consume() {
        //订阅一个topic
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                //1000是等待超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("receive: key = " + record.key() + ", value = " + record.value() + ", offset = " + record.offset());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }

            }
        } finally {
            consumer.close();
        }

    }

    public static void main(String[] args) {
        new Consumer().consume();
    }
}
