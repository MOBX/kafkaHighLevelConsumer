package com.mob.kafkawapper.producer;

import com.lamfire.logger.Logger;
import com.lamfire.utils.RandomUtils;
import com.mob.kafkawapper.cons.KafkaConstant;

/**
 * 消息生产工具类
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class ProducerUtil implements KafkaConstant {

    private static final Logger PRODUCER_LOGGER = Logger.getLogger("producer");

    /**
     * 通用的kafka生产者
     *
     * @param topic topic name
     * @param message 消息内容
     */
    public static void send(String topic, String message, boolean saveLog) {
        MessageProducer producer = null;
        try {
            producer = MessageProducer.getInstance();
            producer.send(topic, PARTITIONS_MULTIPLE_KEY + RandomUtils.nextInt(64), message, saveLog);
        } catch (Exception e) {
            PRODUCER_LOGGER.error("ProducerUtil exception.[" + topic + "][" + message + "]", e);
        }
    }

    /**
     * 发送需要固定顺序的消息
     *
     * @param topic topic name
     * @param message 消息内容
     */
    public static void sendSingleton(String topic, String message, boolean saveLog) {
        MessageProducer producer = null;
        try {
            producer = MessageProducer.getInstance();
            producer.send(topic, PARTITIONS_SINGLETON_KEY, message, saveLog);
        } catch (Exception e) {
            PRODUCER_LOGGER.error("ProducerUtil exception.[" + topic + "][" + message + "]", e);
        }
    }
}
