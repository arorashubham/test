package com.test;

public class Main {

    public static void main(String[] args) {
        Queue q = new QueueImpl();
        try {
            String topicName = "topic123";
            String consumer1Name = "consumer1";
            String consumer2Name = "consumer2";
            String consumerGroup = "consumerGroup";
            q.addTopic(topicName, 5);

            q.addData(topicName, 0, "data");
            q.addData(topicName, 0, "data");
            q.addData(topicName, 0, "data");
            q.addData(topicName, 0, "data");
            q.addData(topicName, 1, "data");
            q.addData(topicName, 1, "data");
            q.addData(topicName, 1, "data");
            q.addData(topicName, 1, "data");

            Consumer consumer1 = new Consumer(topicName, q, consumer1Name, consumerGroup);
            Consumer consumer2 = new Consumer(topicName, q,  consumer2Name, consumerGroup);
            new Thread(consumer1).start();
            new Thread(consumer2).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
