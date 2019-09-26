package ca.radiant3.redisq.consumer;

import ca.radiant3.redisq.MessageQueue;
import ca.radiant3.redisq.persistence.RedisOps;

/**
 * 消费者心跳策略，每个消费者会定时（60s）上报自己的状态，
 * 因为容器场景下hostname每次部署都会变，这样生产者在发送消息的时候对没有上报的消费者可以做剔除
 *
 * @author wangll
 */
public class HeartbeatStrategy extends Thread {


    private RedisOps redisOps;

    private MessageQueue queue;

    HeartbeatStrategy(RedisOps redisOps, MessageQueue queue) {
        this.redisOps = redisOps;
        this.queue = queue;
    }

    @Override
    public void run() {
        redisOps.updateConsumerRegistered(queue.getQueueName(), queue.getDefaultConsumerId());
        try {
            Thread.sleep(redisOps.getHeartbeatInterval());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
