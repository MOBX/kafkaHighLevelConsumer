/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lamfire.json.JSON;
import com.lamfire.utils.PropertiesUtils;

/**
 * 消息生产
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class MessageProducer {

    private static final Logger          PRODUCER_LOGGER  = LoggerFactory.getLogger("producer");
    private static final String          default_producer = "kafka-producer.properties";

    private static final MessageProducer instance         = new MessageProducer();

    public static MessageProducer getInstance() {
        return instance;
    }

    private Properties               props;
    private ProducerConfig           config;
    private Producer<String, String> producer;

    private MessageProducer() {
        props = PropertiesUtils.load(default_producer, MessageProducer.class);
        config = new ProducerConfig(props);
    }

    public void send(String topicName, String key, String message, boolean saveLog) {
        try {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, key, message);
            getProducer().send(data);
            if (saveLog) {
                PRODUCER_LOGGER.info(buildData(topicName, key, message));
            }
        } catch (Exception e) {
            PRODUCER_LOGGER.error(buildData(topicName, key, message), e);
        }
    }

    public void send(String topicName, String message, boolean saveLog) {
        KeyedMessage<String, String> data = null;
        try {
            data = new KeyedMessage<String, String>(topicName, message);
            getProducer().send(data);
            if (saveLog) {
                PRODUCER_LOGGER.info(buildData(topicName, data.partitionKey(), message));
            }
        } catch (Exception e) {
            PRODUCER_LOGGER.error(buildData(topicName, null, message), e);
        }
    }

    private synchronized Producer<String, String> getProducer() {
        if (producer == null) {
            producer = new Producer<String, String>(config);
        }
        return producer;
    }

    public synchronized void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    private String buildData(String topicName, Object key, String message) {
        JSON json = new JSON();
        json.put("topic", topicName);
        json.put("time", System.currentTimeMillis());
        json.put("partitionKey", key);
        json.put("message", message);
        return json.toJSONString();
    }
}
