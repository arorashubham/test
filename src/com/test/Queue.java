package com.test;

import java.util.List;

public interface Queue {
    void addData(String topic, Integer partition, String data) throws Exception;
    void addTopic(String topic, Integer noOfPartitions) throws Exception;
    int registerConsumer(Consumer consumer) throws Exception;
    List<String> request(Consumer consumer, Integer capacity) throws Exception;
    void deregister(String topic, String consumerGroup, String consumerName) throws Exception;
}
