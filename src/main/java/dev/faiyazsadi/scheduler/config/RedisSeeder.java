package dev.faiyazsadi.scheduler.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamConsumerInfo;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisSeeder implements CommandLineRunner {

    private final JedisPool pool;

    @Value("#{streamName}")
    String STREAM_NAME;

    @Value("#{consumerGroupName}")
    String GROUP_NAME;

    @Value("#{uniqueSchedulerId}")
    String CONSUMER_NAME;

    @Override
    public void run(String... args) {
        try (Jedis jedis = pool.getResource()) {

            if(!jedis.exists(STREAM_NAME)) {
                jedis.xgroupCreate(STREAM_NAME, GROUP_NAME, new StreamEntryID(), true);
            }

            boolean groupExists = false;
            List<StreamGroupInfo> groupInfo = jedis.xinfoGroups(STREAM_NAME);
            for (StreamGroupInfo group : groupInfo) {
                System.out.println(group.getGroupInfo().containsValue(GROUP_NAME));
                groupExists |= group.getGroupInfo().containsValue(GROUP_NAME);
            }

            if (!groupExists) {
                String groupStatus = jedis.xgroupCreate(STREAM_NAME, GROUP_NAME, new StreamEntryID(), true);
                System.out.println("Group Created! " + groupStatus);
            }

            boolean consumerExists = false;
            List<StreamConsumerInfo> groupConsumerInfo = jedis.xinfoConsumers2(STREAM_NAME, GROUP_NAME);
            for (StreamConsumerInfo groupConsumer : groupConsumerInfo) {
                consumerExists |= groupConsumer.getConsumerInfo().containsValue(CONSUMER_NAME);
            }

            if (!consumerExists) {
                boolean consumerStatus = jedis.xgroupCreateConsumer(STREAM_NAME, GROUP_NAME, CONSUMER_NAME);
                System.out.println("Consumer Created! " + consumerStatus);
            }
        }
    }
}
