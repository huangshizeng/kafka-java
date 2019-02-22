package com.huang.kafka.helloworld;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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
        //是否自动提交，默认为true
        props.put("enable.auto.commit", "true");
        //从poll(拉)的回话处理时长
        props.put("auto.commit.interval.ms", "1000");
        //键序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //值序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void consume() {
        //订阅一个topic
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("receive: key = " + record.key() + ", value = " + record.value() + ", offset===" + record.offset());
            }
        }
    }

    public static void main(String[] args) {
        new Consumer().consume();
    }
}
