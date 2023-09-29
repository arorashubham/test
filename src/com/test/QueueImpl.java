package com.test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueueImpl implements Queue{

    Map<String, Map<Integer, List<ListNode>> > topicData = new ConcurrentHashMap();
    Map<String, Map<String, List<Consumer>>> consumersData = new ConcurrentHashMap<>();
    Map<String, Map<Integer,OffsetData>> offsetDetails = new ConcurrentHashMap<>();

    @Override
    public void addData(String topic, Integer partition, String data) throws Exception {
        if(!topicData.containsKey(topic) || !topicData.get(topic).containsKey(partition))
            throw new Exception("Invalid topicName/partition");
        topicData.get(topic).get(partition).add(new ListNode(topicData.get(topic).get(partition).size(),data));
        consumersData.put(topic, new ConcurrentHashMap<>());
    }

    @Override
    public void addTopic(String topic, Integer noOfPartitions) throws Exception {
        if(topicData.containsKey(topic)) throw new Exception(String.format("Topic %s already exists", topic));
        Map<Integer, List<ListNode>> map = new ConcurrentHashMap<>();
        IntStream.range(0,noOfPartitions).forEach(p-> map.put(p, new ArrayList<>()));
        topicData.put(topic, map);
    }

    @Override
    public int registerConsumer(Consumer consumer) throws Exception {
        if(!topicData.containsKey(consumer.getTopic())) throw new Exception("Topic doesn't exist");
        if(consumersData.get(consumer.getTopic()).containsKey(consumer.getConsumerGroup()) && consumersData.get(consumer.getTopic()).get(consumer.getConsumerGroup()).contains(consumer.getConsumerName()))
            throw new Exception("Consumer already registered");
        if(consumersData.get(consumer.getTopic()).getOrDefault(consumer.getConsumerGroup(), new ArrayList<>()).size()>=topicData.get(consumer.getTopic()).size())
            throw new Exception("Consumers cannot be more than the No of partitions");
        consumersData.get(consumer.getTopic()).computeIfAbsent(consumer.getConsumerGroup(), (k)-> new ArrayList<>()).add(consumer);
        int partition = (consumersData.get(consumer.getTopic()).get(consumer.getConsumerGroup()).size()-1);
        List<ListNode> data = topicData.get(consumer.getTopic()).get(partition);
        offsetDetails.computeIfAbsent(consumer.getConsumerGroup(),(v)-> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, v->new OffsetData(0, data.get(0)));
        return partition;
    }

    @Override
    public List<String> request(Consumer consumer, Integer capacity) throws Exception {
        if(!topicData.containsKey(consumer.getTopic())) throw new Exception(String.format("Topic %s doesn't exists", consumer.getTopic()));
        if(!consumersData.get(consumer.getTopic()).get(consumer.getConsumerGroup()).contains(consumer))
            throw new Exception("Consumer is not registered");
        List<String> data = new ArrayList<>();
        List<ListNode> dataInTopicPartition = topicData.get(consumer.getTopic()).get(consumer.getPartition());
        int currentIndex = dataInTopicPartition.indexOf(offsetDetails.get(consumer.getConsumerGroup()).get(consumer.getPartition()).getNode());
        int i = 0;
        for (i = 0; i < capacity && (currentIndex + i) < dataInTopicPartition.size(); i++) {
            data.add(dataInTopicPartition.get(currentIndex + i).getData());
        }
        offsetDetails.get(consumer.getConsumerGroup()).put(consumer.getPartition(), new OffsetData(currentIndex + i-1, dataInTopicPartition.get(currentIndex+ i-1)));
        return data;
    }

    @Override
    public void deregister(String topic, String consumerGroup, String consumerName) throws Exception {
        if(!consumersData.containsKey(topic) || !consumersData.get(topic).containsKey(consumerGroup) ||!consumersData.get(topic).get(consumerGroup).contains(consumerName))
            throw  new Exception("Topic/ConsumerGroup/Consumer is not registered");
        List<Consumer> consumers = consumersData.get(topic).get(consumerGroup);
        consumersData.get(topic).get(consumerGroup).remove(IntStream.range(0, consumers.size()).filter(i -> consumers.get(i).getConsumerName().equals(consumerName)).boxed().collect(Collectors.toList()));
    }

    class ListNode {
        private int offsetId;
        private String data;

        public ListNode(int offsetId, String data) {
            this.offsetId = offsetId;
            this.data = data;
        }

        public int getOffsetId() {
            return offsetId;
        }

        public String getData() {
            return data;
        }
    }

    class OffsetData {
        private int offset;
        private ListNode node;

        public OffsetData(int offset, ListNode node) {
            this.offset = offset;
            this.node = node;
        }

        public int getOffset() {
            return offset;
        }

        public ListNode getNode() {
            return node;
        }
    }

}
