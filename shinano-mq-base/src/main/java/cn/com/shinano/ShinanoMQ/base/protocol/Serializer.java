package cn.com.shinano.ShinanoMQ.base.protocol;

import cn.com.shinano.ShinanoMQ.base.util.ProtostuffUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

public interface Serializer {
    <T> T deserialize(byte[] bytes, Class<T> aClass);

    <T> byte[] serialize(T object);

    <T> List<T> deserializeList(byte[] bytes, Class<T> aClass);

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

            @Override
            public <T> List<T> deserializeList(byte[] bytes, Class<T> aClass) {
                throw new IllegalStateException("Protostuff didn't support deserializeList method");
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
            public <T> List<T> deserializeList(byte[] bytes, Class<T> aClass) {
                Object parse = JSONArray.parse(bytes);
                return JSONArray.parseArray(parse.toString(), aClass);
            }
        }
    }
}
