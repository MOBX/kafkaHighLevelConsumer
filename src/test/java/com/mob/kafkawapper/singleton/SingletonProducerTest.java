package com.mob.kafkawapper.singleton;

import com.mob.kafkawapper.producer.ProducerUtil;

/**
 * @author zxc Jul 26, 2017 3:09:34 PM
 */
public class SingletonProducerTest {

    public static void main(String[] args) {
        try {
            for (int i = 0; i < 100; i++) {
                String temp = "singleton_topic_content_" + i;
                ProducerUtil.sendSingleton("singleton_topic", temp, false);
                Thread.sleep(20);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
