package ca.radiant3.redisq.utils;

public class KeysFactory {

    /**
     * Set结构存放 消费者列表，通配符是队列的名称
     */
    private static final String REGISTERED_CONSUMERS_KEY_PATTERN = "redisq.%s.consumers";
    /**
     * Hash结构存放实际的消息体，第一个通配符是队列的名称，第二个通配符是消息的自增值
     */
    private static final String MESSAGE_KEY_PATTERN = "redisq.%s.messages.%s";
    /**
     * List结构，这货使用的地方比较多会
     * 当作为生产者消息的key时，第一个通配符是队列的名称，第二个是生产者的customerId，存储无消费者的情况下发送的消息id
     * 当作为消费者消息的key时，第一个通配符是队列的名称，第二个是消费者的customerId，存储消费者为正确获取的消息id
     */
    private static final String QUEUE_PATTERN = "redisq.%s.queues.%s";
    /**
     * K/V结构存放下一个消息的自增值，其实存的并不是下一个的而是已经发了的最大的自增值
     */
    private static final String NEXT_MESSAGE_ID_PATTERN = "redisq.%s.nextID";
    /**
     * 不知道干啥的，估计是在LockingQueueDequeueStrategy策略中会用到
     */
    private static final String LOCK_SUFFIX = ".lock";
    /**
     * 这个就更不知道是干啥的了
     */
    private static final String NOTIF_LIST_SUFFIX = ".notifs";

    public static String keyForConsumerSpecificQueue(String queue, String consumerId) {
        return String.format(QUEUE_PATTERN, queue, consumerId);
    }

    public static String keyForConsumerSpecificQueueNotificationList(String queue, String consumerId) {
        return keyForConsumerSpecificQueue(queue, consumerId) + NOTIF_LIST_SUFFIX;
    }

    public static String keyForConsumerSpecificQueueLock(String queue, String consumerId) {
        return keyForConsumerSpecificQueue(queue, consumerId) + LOCK_SUFFIX;
    }

    public static String keyForMessage(String queueName, String messageId) {
        return String.format(MESSAGE_KEY_PATTERN, queueName, messageId);
    }

    public static String keyForRegisteredConsumers(String queue) {
        return String.format(REGISTERED_CONSUMERS_KEY_PATTERN, queue);
    }

    public static String keyForNextID(String queue) {
        return String.format(NEXT_MESSAGE_ID_PATTERN, queue);
    }
}
