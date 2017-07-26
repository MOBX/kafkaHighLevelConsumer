/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.singleton;

import com.mob.kafkawapper.consumer.MessageConsumer;
import com.mob.kafkawapper.consumer.MessageExecutor;

/**
 * @author zxc Jul 26, 2017 3:09:27 PM
 */
public class SingletonConsumerTest {

    public static void main(String[] args) {
        MessageConsumer consumer = new MessageConsumer("singleton_topic", 3);
        consumer.setExecutor(new MessageExecutor() {

            @Override
            public void execute(String message) {
                System.out.println(message);
            }
        });
        consumer.run();
    }
}
