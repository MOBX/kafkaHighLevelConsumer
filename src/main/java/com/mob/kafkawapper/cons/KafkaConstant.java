/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafkawapper.cons;

/**
 * kafka使用相关的常量(topic名,key名等),方便统一管理topic域空间
 *
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public interface KafkaConstant {

    /**
     * 发送无序消息的key
     */
    public static final String PARTITIONS_MULTIPLE_KEY  = "multiple_";
    /**
     * 发送有序的消息key
     */
    public static final String PARTITIONS_SINGLETON_KEY = "singleton_";

}
