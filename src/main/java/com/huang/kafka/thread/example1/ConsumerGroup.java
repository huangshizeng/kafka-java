package com.huang.kafka.thread.example1;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄世增
 */

public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        consumers.forEach(c -> new Thread(c).start());
    }
}
