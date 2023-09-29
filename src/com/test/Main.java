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

            q.addData(topicName, 0, "Parition 0 data 0");
            q.addData(topicName, 0, "Parition 0 data 1");
            q.addData(topicName, 0, "Parition 0 data 2");
            q.addData(topicName, 0, "Parition 0 data 3");
            q.addData(topicName, 1, "Parition 1 data 0");
            q.addData(topicName, 1, "Parition 1 data 1");
            q.addData(topicName, 1, "Parition 1 data 2");
            q.addData(topicName, 1, "Parition 1 data 3");

            Consumer consumer1 = new Consumer(topicName, q, consumerGroup, consumer1Name);
            Consumer consumer2 = new Consumer(topicName, q,  consumerGroup, consumer2Name);

//            Consumer consumer1 = new Consumer(topicName, q, consumer1Name, consumerGroup);
//            Consumer consumer2 = new Consumer(topicName, q,  consumer2Name, consumerGroup);
            new Thread(consumer1).start();
            new Thread(consumer2).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
