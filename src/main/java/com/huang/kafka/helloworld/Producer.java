package com.huang.kafka.helloworld;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Properties;

/**
 * @author 黄世增
 */

public class Producer {

    private final KafkaProducer<String, String> producer;
    private static final String topic = "my-topic";

    public Producer() {
        Properties props = new Properties();
        //kafka的地址
        props.put("bootstrap.servers", "localhost:9092");
        //acks:消息的确认机制，默认值是0。
        //acks=0：如果设置为0，生产者不会等待kafka的响应。
        //acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        //acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这是最强的可用性保证
        props.put("acks", "all");
        //配置为大于0的值的话，客户端会在消息发送失败时重新发送
        props.put("retries", 0);
        //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高client和生产者的效率
        props.put("batch.size", 16384);
        //键序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //值序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public void produce() {
        for (int i = 0; i < 100; i++) {
            String message = "你好，这是第" + (i + 1) + "条数据";
            ProducerRecord record = new ProducerRecord<>("my-topic", message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("消息：'" + metadata.toString() + "' 发送成功");
                } else {
                    if (exception instanceof RetriableException) {
                        System.out.println("处理可重试瞬时异常");
                    } else {
                        System.out.println("处理不可重试异常");
                    }
                }
            });
            System.out.println("发送的消息：" + message);
        }
        producer.close();
    }

    public static void main(String[] args) {
        new Producer().produce();
    }
}
