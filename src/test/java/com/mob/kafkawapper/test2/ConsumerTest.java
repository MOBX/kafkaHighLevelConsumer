/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.test2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.lamfire.logger.Logger;
import com.lamfire.utils.Threads;
import com.mob.kafkawapper.consumer.MessageConsumer;
import com.mob.kafkawapper.consumer.MessageExecutor;

/**
 * @author zxc Jul 26, 2017 3:10:45 PM
 */
public class ConsumerTest {

    private static final Logger                       LOGGER = Logger.getLogger("test_log");
    private static ConcurrentHashMap<String, Integer> map    = new ConcurrentHashMap<String, Integer>();

    public static void main(String[] args) {
        analysisConsumer();
        Threads.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    LOGGER.info(entry.getKey() + " : " + entry.getValue());
                }
            }
        }, 15, 10, TimeUnit.SECONDS);

    }

    private static void analysisConsumer() {
        MessageConsumer consumer = new MessageConsumer("testinner_topic", 5);
        consumer.setExecutor(new MessageExecutor() {

            @Override
            public void execute(String message) {
                Integer step = map.get(message);
                if (step == null) {
                    map.put(message, 1);
                } else {
                    step += 1;
                    map.put(message, step);
                }
            }
        });
        consumer.run();
    }
}
