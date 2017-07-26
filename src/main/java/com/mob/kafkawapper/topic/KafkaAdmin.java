/*
 * Copyright 2017-2020 mob.com All right reserved.
 */
package com.mob.kafkawapper.topic;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;

import org.I0Itec.zkclient.ZkClient;

import com.lamfire.logger.Logger;
import com.lamfire.utils.StringUtils;

/**
 * kafka管理类
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class KafkaAdmin {

    private static final Logger _ = Logger.getLogger(KafkaAdmin.class);
    private String              zkConnect;
    private int                 sessionTimeout;
    private int                 connectionTimeout;

    public KafkaAdmin() throws Exception {
        String zkFile = "kafka-consumer.properties";
        Properties pro = new Properties();

        InputStream is = null;
        try {
            is = KafkaAdmin.class.getClassLoader().getResourceAsStream(zkFile);
        } catch (Exception e) {
            throw new Exception("ClassLoader " + zkFile + " exception.");
        }
        try {
            pro.load(is);
        } catch (IOException e) {
            throw new Exception("Properties load " + zkFile + " exception.");
        }

        this.zkConnect = pro.getProperty("zookeeper.connect");
        String timeout = pro.getProperty("zookeeper.connection.timeout.ms");
        this.connectionTimeout = StringUtils.isBlank(timeout) ? 300000 : Integer.parseInt(timeout);
        this.sessionTimeout = connectionTimeout;
    }

    public KafkaAdmin(String zkConnect, int sessionTimeout, int connectionTimeout) {
        this.zkConnect = zkConnect;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * 创建topic
     *
     * @param topicName 名称
     * @param replication 副本的个数
     * @param partitions partition数量
     * @throws Exception
     */
    public void createTopic(String topicName, int replication, int partitions) throws Exception {
        if (StringUtils.isBlank(topicName)) {
            throw new Exception("TopicName is null.");
        }
        if (replication < 1) {
            throw new Exception("Replication value must be greater than 0.");
        }
        if (partitions < 1) {
            throw new Exception("Partitions value must be greater than 0.");
        }
        String[] array = new String[6];
        array[0] = "--replication-factor";
        array[1] = String.valueOf(replication);
        array[2] = "--partitions";
        array[3] = String.valueOf(partitions);
        array[4] = "--topic";
        array[5] = topicName;

        _.debug("[createTopic] array=" + array);
        if (StringUtils.isBlank(this.zkConnect)) {
            throw new Exception("zkConnect value is null.");
        }

        ZkClient zkClient = null;
        try {
            zkClient = getClient();
            TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(array);
            TopicCommand.createTopic(zkClient, options);
        } finally {
            zkClient.close();
        }
    }

    /**
     * 删除topic
     *
     * @param name topicName
     */
    public void removeTopic(String name) throws Exception {
        if (StringUtils.isBlank(name)) {
            throw new Exception("TopicName is null.");
        }
        ZkClient zkClient = null;
        try {
            zkClient = getClient();
            AdminUtils.deleteTopic(zkClient, name);
        } finally {
            zkClient.close();
        }
    }

    /**
     * 检查topic是否已经存在
     * 
     * @param name topic name
     * @return boolean
     * @throws Exception
     */
    public Boolean topicExists(String name) throws Exception {
        if (StringUtils.isBlank(name)) {
            throw new Exception("TopicName is null.");
        }
        ZkClient zkClient = null;
        try {
            zkClient = getClient();
            return AdminUtils.topicExists(zkClient, name);
        } finally {
            zkClient.close();
        }
    }

    private ZkClient getClient() {
        return new ZkClient(zkConnect, sessionTimeout, connectionTimeout, new ZKStringSerialize());
    }
}
