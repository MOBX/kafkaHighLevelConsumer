/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lamfire.json.JSON;

/**
 * 消费者处理类
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class ConsumerExecutor implements Runnable {

    private static final Logger         CONSUMER_LOGGER = LoggerFactory.getLogger("consumer");

    private KafkaStream<byte[], byte[]> kafkaStream;
    private MessageExecutor             executor;
    private boolean                     saveLog         = false;

    public ConsumerExecutor(KafkaStream<byte[], byte[]> kafkaStream, MessageExecutor executor, boolean saveLog) {
        this.kafkaStream = kafkaStream;
        this.executor = executor;
        this.saveLog = saveLog;
    }

    public ConsumerExecutor(KafkaStream<byte[], byte[]> kafkaStream, MessageExecutor executor) {
        this.kafkaStream = kafkaStream;
        this.executor = executor;
    }

    @Override
    public void run() {
        MessageAndMetadata<byte[], byte[]> next = null;
        String msg = null;
        try {
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                next = iterator.next();
                msg = new String(next.message());
                executor.execute(msg);
                if (saveLog) {
                    CONSUMER_LOGGER.info(buildData(next.topic(), next.partition(), next.offset(), msg));
                }
            }
        } catch (Exception e) {
            CONSUMER_LOGGER.error(e.toString(), e);
        }
    }

    private String buildData(String topicName, int partition, long offset, String message) {
        JSON json = new JSON();
        json.put("topic", topicName);
        json.put("time", System.currentTimeMillis());
        json.put("partition", partition);
        json.put("offset", offset);
        json.put("message", message);
        return json.toJSONString();
    }
}
