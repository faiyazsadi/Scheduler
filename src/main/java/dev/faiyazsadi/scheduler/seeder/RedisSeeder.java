package dev.faiyazsadi.scheduler.seeder;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.resps.StreamConsumerInfo;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Component
@Order(1)
public class RedisSeeder implements CommandLineRunner {

    private final JedisPool pool;

    @Override
    public void run(String... args) {

        String STREAM_NAME = "JOBS";
        String GROUP_NAME  = "EXECUTORS";
        String CONSUMER1 = "EXECUTOR-1";
        String CONSUMER2 = "EXECUTOR-2";

        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();

            if (!jedis.exists(STREAM_NAME)) {
                for (int i = 1; i <= 20; ++i) {
                    jedis.xadd(STREAM_NAME,
                        Map.of(
                            "jobName", "job-" + i,
                            "projectName", "project-" + i,
                            "jobStatus", "starting"
                        ),
                        XAddParams.xAddParams()
                    );
                }
            }

            boolean groupExists = false;
            List<StreamGroupInfo> groupInfo = jedis.xinfoGroups(STREAM_NAME);
            for (StreamGroupInfo group : groupInfo) {
                System.out.println(group.getGroupInfo().containsValue(GROUP_NAME));
                groupExists |= group.getGroupInfo().containsValue(GROUP_NAME);
            }
            if (!groupExists) {
                String groupStatus = jedis.xgroupCreate(STREAM_NAME, GROUP_NAME, new StreamEntryID(), false);
                System.out.println("Group Created! " + groupStatus);
            }

            boolean consumerExists = false;
            List<StreamConsumerInfo> groupConsumerInfo = jedis.xinfoConsumers2(STREAM_NAME, GROUP_NAME);
            for (StreamConsumerInfo groupConsumer : groupConsumerInfo) {
                consumerExists |= groupConsumer.getConsumerInfo().containsValue(CONSUMER1);
                consumerExists |= groupConsumer.getConsumerInfo().containsValue(CONSUMER2);
            }
            if (!consumerExists) {
                boolean consumerStatus1 = jedis.xgroupCreateConsumer(STREAM_NAME, GROUP_NAME, CONSUMER1);
                boolean consumerStatus2 = jedis.xgroupCreateConsumer(STREAM_NAME, GROUP_NAME, CONSUMER2);

                System.out.println("Consumer Created! " + consumerStatus1);
                System.out.println("Consumer Created! " + consumerStatus2);
            }
        }
    }
}
