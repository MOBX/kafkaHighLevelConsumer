package com.mob.kafkawapper.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.lamfire.utils.PropertiesUtils;

/**
 * kafka消费者
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class MessageConsumer implements Runnable {

    private static Properties pro     = PropertiesUtils.load("kafka-consumer.properties", MessageConsumer.class);

    private String            topic;
    private int               streams = 1;

    private ConsumerConfig    config;
    private ConsumerConnector connector;

    private ExecutorService   threadPool;
    private MessageExecutor   executor;
    private boolean           saveLog = false;

    public MessageConsumer(String topic, int streams, boolean saveLog) {
        this.topic = topic;
        this.streams = streams;
        this.saveLog = saveLog;
        this.threadPool = Executors.newFixedThreadPool(streams);
        this.config = new ConsumerConfig(pro);
    }

    /**
     * @param topic 主题名称
     * @param streams 消费者线程数
     */
    public MessageConsumer(String topic, int streams) {
        this.topic = topic;
        this.streams = streams;
        this.threadPool = Executors.newFixedThreadPool(streams);
        this.config = new ConsumerConfig(pro);
    }

    public MessageConsumer(String topic, int streams, String configFile) {
        this.topic = topic;
        this.streams = streams;
        this.threadPool = Executors.newFixedThreadPool(streams);
        this.config = new ConsumerConfig(PropertiesUtils.load(configFile, MessageConsumer.class));
    }

    private synchronized ConsumerConnector getConsumer() {
        if (connector == null) {
            connector = Consumer.createJavaConsumerConnector(config);
        }
        return connector;
    }

    public synchronized void close() {
        if (connector != null) {
            connector.shutdown();
            threadPool.shutdown();
            connector = null;
            threadPool = null;
        }
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, this.streams);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = getConsumer().createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> partitions = consumerMap.get(topic);

        for (KafkaStream<byte[], byte[]> partition : partitions) {
            threadPool.execute(new ConsumerExecutor(partition, executor, saveLog));
        }
    }

    public boolean isSaveLog() {
        return saveLog;
    }

    public void setSaveLog(boolean saveLog) {
        this.saveLog = saveLog;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getStreams() {
        return streams;
    }

    public void setStreams(int streams) {
        this.streams = streams;
    }

    public MessageExecutor getExecutor() {
        return executor;
    }

    public void setExecutor(MessageExecutor executor) {
        this.executor = executor;
    }
}
