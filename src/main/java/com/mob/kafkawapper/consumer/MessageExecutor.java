package com.mob.kafkawapper.consumer;

/**
 * 消息回调接口
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public interface MessageExecutor {

    public void execute(String message);
}
