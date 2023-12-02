package cn.com.shinano.ShinanoMQ.base.protocol;

import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import com.alibaba.fastjson.JSONObject;

public interface Serializer {
    <T> T deserialize(byte[] bytes, Class<T> aClass);

    <T> byte[] serialize(T object);

    enum Algorithm implements Serializer {
        Protostuff {
            @Override
            public <T> T deserialize(byte[] bytes, Class<T> aClass) {
                return ProtostuffUtils.deserialize(bytes, aClass);
            }

            @Override
            public <T> byte[] serialize(T object) {
                return ProtostuffUtils.serialize(object);
            }
        },
        JSON {
            @Override
            public <T> T deserialize(byte[] bytes, Class<T> aClass) {
                return JSONObject.parseObject(bytes, aClass);
            }

            @Override
            public <T> byte[] serialize(T object) {
                return JSONObject.toJSONBytes(object);
            }
        }
    }
}
