/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.test1;

import java.util.concurrent.atomic.AtomicInteger;

import com.lamfire.logger.Logger;
import com.mob.kafkawapper.consumer.MessageConsumer;
import com.mob.kafkawapper.consumer.MessageExecutor;

/**
 * @author zxc Jul 26, 2017 3:09:09 PM
 */
public class ConsumerTest {

    private static final Logger  LOGGER = Logger.getLogger(ConsumerTest.class);

    private static AtomicInteger total  = new AtomicInteger(0);

    public static void main(String[] args) {
        MessageConsumer consumer = new MessageConsumer("test_encode_topic", 2);
        consumer.setExecutor(new MessageExecutor() {

            @Override
            public void execute(final String message) {
                LOGGER.info(total.incrementAndGet() + " : " + Thread.currentThread().getName() + " :  " + message);
            }
        });
        consumer.run();
    }
}
