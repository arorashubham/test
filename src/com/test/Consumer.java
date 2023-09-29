package com.test;

import java.util.List;

public class Consumer implements Runnable  {

    private String consumerName;
    private String consumerGroup;
    private Queue q;
    private String topic;
    private int partition;

    private Integer capapcity = 100;

    public String getConsumerName() {
        return consumerName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Consumer(String topic, Queue q, String consumerGroup, String consumerName) throws Exception {
        this.q = q;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.consumerName = consumerName;
        this.partition = q.registerConsumer(this);
    }

    public void consume(Consumer consumer, Integer capacity) throws Exception {
        List<String> data = q.request(this, capacity);
        for(String s: data) {
            System.out.println("data");
        }
    }

    @Override
    public void run() {
        try {
            consume(this, capapcity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
