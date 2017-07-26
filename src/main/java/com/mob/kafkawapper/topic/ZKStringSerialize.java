package com.mob.kafkawapper.topic;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.lamfire.logger.Logger;

/**
 * zk序列化类
 * 
 * @author zxc Oct 30, 2015 5:24:13 PM
 */
public class ZKStringSerialize implements ZkSerializer {

    private static final Logger _ = Logger.getLogger(ZKStringSerialize.class);

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data instanceof String) {
            try {
                return ((String) data).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                _.error("serialize UnsupportedEncodingException", e);
            }
        }
        return null;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if (bytes == null) {
            return null;
        } else try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            _.error("deserialize UnsupportedEncodingException", e);
        }
        return null;
    }
}
