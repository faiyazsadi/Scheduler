package dev.faiyazsadi.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.*;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class SchedulerApplication {


    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);

        String STREAM_NAME = "JOBS";
        String GROUP_NAME  = "EXECUTORS";
        String CONSUMER_NAME = "EXECUTOR-1";
        String HOST_NAME = "127.0.0.1";
        int PORT = 6379;

        JedisPool pool = new JedisPool(HOST_NAME, PORT);

        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();

            if (!jedis.exists(STREAM_NAME)) {
                for (int i = 0; i < 10; ++i) {
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
                consumerExists |= groupConsumer.getConsumerInfo().containsValue(CONSUMER_NAME);
            }
            if (!consumerExists) {
                boolean consumerStatus = jedis.xgroupCreateConsumer(STREAM_NAME, GROUP_NAME, CONSUMER_NAME);
                System.out.println("Consumer Created! " + consumerStatus);
            }

            List<Map.Entry<String, List<StreamEntry>>> message = jedis.xreadGroup(GROUP_NAME, CONSUMER_NAME,
                    XReadGroupParams.xReadGroupParams().count(1),
                    Map.of(STREAM_NAME, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY)
            );
            System.out.println("Message Read Successful! " + message);

            StreamPendingSummary summary =  jedis.xpending(STREAM_NAME, GROUP_NAME);
            System.out.println("Pending Message Count Provided! " + summary.getTotal());
        }
    }
}
