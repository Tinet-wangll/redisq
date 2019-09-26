package ca.radiant3.redisq.persistence;

import static ca.radiant3.redisq.utils.KeysFactory.keyForConsumerSpecificQueue;
import static ca.radiant3.redisq.utils.KeysFactory.keyForConsumerSpecificQueueLock;
import static ca.radiant3.redisq.utils.KeysFactory.keyForConsumerSpecificQueueNotificationList;
import static ca.radiant3.redisq.utils.KeysFactory.keyForMessage;
import static ca.radiant3.redisq.utils.KeysFactory.keyForNextID;
import static ca.radiant3.redisq.utils.KeysFactory.keyForRegisteredConsumers;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;

import ca.radiant3.redisq.Message;
import ca.radiant3.redisq.MessageQueue;
import ca.radiant3.redisq.serialization.DefaultMessageConverter;
import ca.radiant3.redisq.serialization.Jackson2PayloadSerializer;
import ca.radiant3.redisq.serialization.MessageConverter;
import ca.radiant3.redisq.serialization.PayloadSerializer;

public class RedisOps {

    private RedisTemplate redisTemplate;

    @Autowired(required = false)
    private PayloadSerializer payloadSerializer = new Jackson2PayloadSerializer();
    @Autowired(required = false)
    private MessageConverter messageConverter = new DefaultMessageConverter();

    private long heartbeatInterval = 60000L;

    @SuppressWarnings("unchecked")
    public void ensureConsumerRegistered(String queueName, String consumerId) {
        BoundZSetOperations<String, String> ops = redisTemplate.boundZSetOps(keyForRegisteredConsumers(queueName));
        ops.add(consumerId, System.currentTimeMillis());
    }

    @SuppressWarnings("unchecked")
    public void deleteConsumerRegistered(String queueName, String consumerId) {
        BoundZSetOperations<String, String> ops = redisTemplate.boundZSetOps(keyForRegisteredConsumers(queueName));
        ops.remove(consumerId);
    }

    @SuppressWarnings("unchecked")
    public Collection<String> getRegisteredConsumers(String queueName) {
        BoundZSetOperations<String, String> ops = redisTemplate.boundZSetOps(keyForRegisteredConsumers(queueName));
        return ops.range(0, -1);
    }

    @SuppressWarnings("unchecked")
    public void updateConsumerRegistered(String queueName, String consumerId) {
        BoundZSetOperations<String, String> ops = redisTemplate.boundZSetOps(keyForRegisteredConsumers(queueName));
        ops.incrementScore(consumerId, heartbeatInterval);
    }

    @SuppressWarnings("unchecked")
    public void removeExpireRegisteredConsumers(String queueName) {
        BoundZSetOperations<String, String> zSetOps = redisTemplate.boundZSetOps(keyForRegisteredConsumers(queueName));

        Set<String> expireKeys = zSetOps.rangeByScore(0, System.currentTimeMillis() - (heartbeatInterval * 10));

        if (Objects.nonNull(expireKeys) && !expireKeys.isEmpty()) {
            // 移除十分钟以外没有心跳的的消费者
            zSetOps.removeRangeByScore(0, System.currentTimeMillis() - (heartbeatInterval * 10));
            // 移除对应消费者的消息队列
            for (String expireKey : expireKeys) {
                redisTemplate.delete(keyForConsumerSpecificQueue(queueName, expireKey));
            }
        }
    }

    public <T> String addMessage(String queueName, Message<T> message) {
        assert message != null;
        assert message.getTimeToLiveSeconds() != null;

        String msgId = generateNextMessageID(queueName);
        message.setId(msgId);
        if (message.getCreation() == null) {
            message.setCreation(Calendar.getInstance());
        }

        saveMessage(queueName, message);

        return msgId;
    }

    public <T> void saveMessage(String queueName, Message<T> message) {
        assert message != null;
        assert message.getId() != null;
        assert message.getTimeToLiveSeconds() != null;

        Map<String, String> asMap = messageConverter.toMap(message, payloadSerializer);

        String messageKey = keyForMessage(queueName, message.getId());
        redisTemplate.opsForHash().putAll(messageKey, asMap);
        redisTemplate.expire(messageKey, message.getTimeToLiveSeconds(), TimeUnit.SECONDS);
    }

    public <T> Message<T> loadMessageById(String queueName, String id, Class<T> payloadType) {
        String messageKey = keyForMessage(queueName, id);
        BoundHashOperations<String, String, String> ops = redisTemplate.boundHashOps(messageKey);
        Map<String, String> messageData = ops.entries();

        return messageConverter.toMessage(messageData, payloadType, payloadSerializer);
    }

    public String dequeueMessageFromHead(String queueName, String consumerId, long timeoutSeconds) {
        String queueKey = keyForConsumerSpecificQueue(queueName, consumerId);

        BoundListOperations<String, String> ops = redisTemplate.boundListOps(queueKey);
        return ops.leftPop(timeoutSeconds, TimeUnit.SECONDS);
    }

    public <T> Message<T> peekNextMessageInQueue(String queueName, String consumerId, Class<T> payloadType) {
        String queueKey = keyForConsumerSpecificQueue(queueName, consumerId);

        BoundListOperations<String, String> ops = redisTemplate.boundListOps(queueKey);

        String nextId = ops.index(0);
        if (nextId == null) {
            return null;
        }
        return loadMessageById(queueName, nextId, payloadType);
    }

    /**
     * @param rangeStart zero-based index of first item to retrieve
     * @param rangeEnd   zero-based index of last item to retrieve
     */
    public <T> List<Message<T>> peekMessagesInQueue(String queueName, String consumerId, long rangeStart,
                                                    long rangeEnd, Class<T> payloadType) {

        String queueKey = keyForConsumerSpecificQueue(queueName, consumerId);

        BoundListOperations<String, String> ops = redisTemplate.boundListOps(queueKey);

        List<String> messageIds = ops.range(rangeStart, rangeEnd);
        List<Message<T>> messages = new ArrayList<Message<T>>(messageIds.size());
        for (String id : messageIds) {
            messages.add(loadMessageById(queueName, id, payloadType));
        }
        return messages;
    }

    /**
     * Peeks messages in the specified queue (for the default consumer).
     *
     * @param rangeStart zero-based index of first item to retrieve
     * @param rangeEnd   zero-based index of last item to retrieve
     */
    public <T> List<Message<T>> peekMessagesInQueue(MessageQueue queue, long rangeStart, long rangeEnd,
                                                    Class<T> payloadType) {
        return peekMessagesInQueue(queue.getQueueName(), queue.getDefaultConsumerId(), rangeStart, rangeEnd,
                payloadType);
    }

    public void emptyQueue(String queueName) {
        Collection<String> consumerIds = getRegisteredConsumers(queueName);

        for (String consumerId : consumerIds) {
            String queueKey = keyForConsumerSpecificQueue(queueName, consumerId);
            redisTemplate.delete(queueKey);
        }
    }

    @SuppressWarnings("unchecked")
    private String generateNextMessageID(String queueName) {
        return Long.toString(
                redisTemplate.opsForValue().increment(keyForNextID(queueName), 1)
        );
    }

    public Long getQueueSizeForConsumer(String queueName, String consumerId) {
        return redisTemplate.opsForList().size(keyForConsumerSpecificQueue(queueName, consumerId));
    }

    public void enqueueMessageAtTail(String queueName, String consumerId, String messageId) {
        if (StringUtils.isEmpty(messageId)) {
            throw new IllegalArgumentException("Message must have been persisted before being enqueued.");
        }

        String key = keyForConsumerSpecificQueue(queueName, consumerId);

        redisTemplate.opsForList().rightPush(key, messageId);
    }

    public void enqueueMessageInSet(String queueName, String consumerId, String messageId) {
        if (StringUtils.isEmpty(messageId)) {
            throw new IllegalArgumentException("Message must have been persisted before being enqueued.");
        }

        String key = keyForConsumerSpecificQueue(queueName, consumerId);

        redisTemplate.opsForSet().add(key, messageId);
    }

    public void notifyWaitersOnSet(String queueName, String consumerId) {
        String key = keyForConsumerSpecificQueueNotificationList(queueName, consumerId);

        redisTemplate.opsForList().rightPush(key, "x");
    }

    public void waitOnSet(String queueName, String consumerId, long timeoutSeconds) {
        String key = keyForConsumerSpecificQueueNotificationList(queueName, consumerId);

        redisTemplate.opsForList().leftPop(key, timeoutSeconds, TimeUnit.SECONDS);
    }

    public String randomPopFromSet(String queueName, String consumerId) {
        String key = keyForConsumerSpecificQueue(queueName, consumerId);

        BoundSetOperations<String, String> ops = redisTemplate.boundSetOps(key);
        return ops.pop();
    }

    public boolean tryObtainLockForQueue(String queueName, String consumerId, long expirationTimeout, TimeUnit unit) {
        BoundValueOperations<String, Integer> ops =
                redisTemplate.boundValueOps(keyForConsumerSpecificQueueLock(queueName, consumerId));

        boolean lockAcquired = ops.setIfAbsent(1);
        if (lockAcquired) {
            ops.expire(expirationTimeout, unit);
            return true;
        }
        return false;
    }

    public void releaseLockForQueue(String queueName, String consumerId) {
        redisTemplate.delete(keyForConsumerSpecificQueueLock(queueName, consumerId));
    }

    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
