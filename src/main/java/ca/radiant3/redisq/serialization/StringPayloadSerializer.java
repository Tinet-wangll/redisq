package ca.radiant3.redisq.serialization;

import org.apache.commons.lang.ObjectUtils;
import org.springframework.data.redis.serializer.SerializationException;

public class StringPayloadSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) throws SerializationException {
        return ObjectUtils.toString(payload);
    }

    @Override
    public <T> T deserialize(String payload, Class<T> type) throws SerializationException {
        return (T) payload;
    }
}
