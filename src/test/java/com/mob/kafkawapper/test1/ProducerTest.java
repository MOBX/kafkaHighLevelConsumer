/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.test1;

import com.mob.kafkawapper.producer.ProducerUtil;

/**
 * @author zxc Jul 26, 2017 3:09:17 PM
 */
public class ProducerTest {

    public static void main(String[] args) {
        try {
            for (int i = 0; i < 3600; i++) {
                String temp = "new_test_encode_" + i;
                ProducerUtil.send("test_encode_topic", temp, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
