/*
 * Copyright 2015-2020 uuzu.com All right reserved.
 */
package com.mob.kafkawapper.topic;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zxc Mar 15, 2017 5:23:36 PM
 */
public class KafkaTopicCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicCreator.class);

    public static void main(String[] args) {
        String topicName = "zxc_test";
        String zookeeperHosts = "10.18.97.61:2181,10.18.97.62:2181,10.18.97.63:2181";
        String kafkaBrokerHosts = "10.18.97.61:9092,10.18.97.62:9092,10.18.97.63:9092";
        int sessionTimeOut = 10000;
        int connectionTimeOut = 10000;
        LOGGER.info("zookeeperHosts:{}", zookeeperHosts);
        ZkClient zkClient = new ZkClient(zookeeperHosts, sessionTimeOut, connectionTimeOut, ZKStringSerializer$.MODULE$);
        if (!AdminUtils.topicExists(zkClient, topicName)) {
            int replicationFactor = kafkaBrokerHosts.split(",").length;
            AdminUtils.createTopic(zkClient, topicName, 1, replicationFactor, new Properties());
        } else {
            LOGGER.info("{} is available hence no changes are done");
        }
        LOGGER.info("Topic Details:{}", AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient));
    }
}
