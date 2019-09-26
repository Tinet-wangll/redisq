package ca.radiant3.redisq.serialization;

import java.io.IOException;

import org.springframework.data.redis.serializer.SerializationException;

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;

public class Jackson2PayloadSerializer implements PayloadSerializer {
    private ObjectMapper mapper;

    public Jackson2PayloadSerializer() {
        mapper = new ObjectMapper();
        mapper.enableDefaultTyping(DefaultTyping.NON_FINAL, As.PROPERTY);
    }

    @Override
    public String serialize(Object payload) throws SerializationException {
        try {
            return mapper.writeValueAsString(payload);
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }

    @Override
    public <T> T deserialize(String payload, Class<T> type) throws SerializationException {
        if (payload == null) {
            return null;
        }

        try {
            return mapper.readValue(payload, type);
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }
}
