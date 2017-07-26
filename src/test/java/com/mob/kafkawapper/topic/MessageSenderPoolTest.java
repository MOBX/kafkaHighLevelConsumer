package com.mob.kafkawapper.topic;

import org.junit.Test;

import com.lamfire.logger.Logger;
import com.lamfire.utils.RandomUtils;
import com.mob.kafkawapper.producer.MessageProducer;
import com.mob.kafkawapper.producer.ProducerUtil;

/**
 * @author zxc Jul 26, 2017 3:08:30 PM
 */
public class MessageSenderPoolTest {

    private static final Logger LOGGER = Logger.getLogger("test_log");

    @Test
    public void utilTest() {
        for (int i = 0; i < 100; i++) {
            ProducerUtil.send("test_replication", "test_" + i, true);
        }
    }

    @Test
    public void producerTest() {
        try {
            long start = System.currentTimeMillis();
            final MessageProducer producer = MessageProducer.getInstance();
            for (int i = 0; i < 10; i++) {
                final String key = RandomUtils.randomText(12, 12);
                final int size = RandomUtils.nextInt(10000);
                Thread thread = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        for (int j = 0; j < size; j++) {
                            producer.send("test_topic", key, true);
                        }
                    }
                });
                LOGGER.info("Thread name : " + thread.getName() + " executeTime: " + (System.currentTimeMillis() - start) + " key: " + key + " size: " + size);
                thread.start();
            }
            producer.close();
            LOGGER.debug("Time consuming: " + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
