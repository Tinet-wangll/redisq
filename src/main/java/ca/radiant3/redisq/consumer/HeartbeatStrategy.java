package ca.radiant3.redisq.consumer;

import ca.radiant3.redisq.MessageQueue;
import ca.radiant3.redisq.persistence.RedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

/**
 * 消费者心跳策略，每个消费者会定时（60s）上报自己的状态，
 * 因为容器场景下hostname每次部署都会变，这样生产者在发送消息的时候对没有上报的消费者可以做剔除
 *
 * @author wangll
 */
public class HeartbeatStrategy extends Thread {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatStrategy.class);

    private RedisOps redisOps;

    private String queueName;

    private String consumerId;


    HeartbeatStrategy(RedisOps redisOps, String queueName, String consumerId) {
        this.redisOps = redisOps;
        this.queueName = queueName;
        this.consumerId = consumerId;
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {

            try {
                Thread.sleep(redisOps.getHeartbeatInterval());

                log.debug("updateConsumerRegistered QueueName: {} ConsumerId: {}", queueName, consumerId);
                redisOps.updateConsumerRegistered(queueName, consumerId);

            } catch (InterruptedException e) {
                log.info("InterruptedException invoke interrupt");
                this.interrupt();
            }
        }
    }
}
