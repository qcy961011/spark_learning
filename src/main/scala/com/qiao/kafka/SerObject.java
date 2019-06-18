package com.qiao.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class SerObject implements Serializer {

    // 如果需要读取固定的配置文件，可以使用这个方法，也可以没有方法体
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    // 进行数据序列化的主要方法
    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        } else {
            byte[] bytes = null;
            ObjectOutputStream bo = null;
            ByteArrayOutputStream oo = null;
            try {
                oo = new ByteArrayOutputStream();
                bo = new ObjectOutputStream(oo);
                bo.writeObject(data);
                bytes = oo.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    bo.close();
                    oo.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return bytes;
        }
    }

    // 最后执行的方法，可以没有方法体
    @Override
    public void close() {

    }
}
