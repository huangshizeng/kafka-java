package com.huang.kafka.thread.example1;

/**
 * 多线程消费实例，每个线程维护一个KafkaConsumer
 *
 * @author 黄世增
 */

public class ConsumerMain {

    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}
