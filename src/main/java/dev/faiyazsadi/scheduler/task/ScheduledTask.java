package dev.faiyazsadi.scheduler.task;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamConsumerInfo;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Component
public class ScheduledTask {

    private final JedisPool pool;

    @Scheduled(fixedRate = 5000)
    public void runTask() {
        System.out.println("Task is running every 5 seconds: " + System.currentTimeMillis());

        String STREAM_NAME = "JOBS";
        String GROUP_NAME  = "EXECUTORS";
        String CONSUMER1 = "EXECUTOR-1";
        String CONSUMER2 = "EXECUTOR-2";

        try (Jedis jedis = pool.getResource()) {

            List<Map.Entry<String, List<StreamEntry>>> message1 = readMessage(jedis, GROUP_NAME, CONSUMER1, STREAM_NAME);
            printMessageInfo(message1, jedis);

            List<Map.Entry<String, List<StreamEntry>>> message2 = readMessage(jedis, GROUP_NAME, CONSUMER2, STREAM_NAME);
            printMessageInfo(message2, jedis);
        }
    }

    private static List<Map.Entry<String, List<StreamEntry>>> readMessage(Jedis jedis, String GROUP_NAME, String CONSUMER, String STREAM_NAME) {
        List<Map.Entry<String, List<StreamEntry>>> message =
                jedis.xreadGroup(GROUP_NAME, CONSUMER,
                XReadGroupParams.xReadGroupParams().count(1),
                Map.of(STREAM_NAME, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY)
        );

        if (message == null) {
            return null;
        }

        System.out.println("Message Read Successful By -> " + CONSUMER);
        return message;
    }

    private static void printMessageInfo(List<Map.Entry<String, List<StreamEntry>>> message, Jedis jedis) {
        if (message == null) {
            return;
        }

        // Process the messages
        for (Map.Entry<String, List<StreamEntry>> entry : message) {
            List<StreamEntry> streamEntries = entry.getValue();

            for (StreamEntry streamEntry : streamEntries) {
                // Get the entry ID
                StreamEntryID entryId = streamEntry.getID();

                HashMap<String, String> hashMap = new HashMap<>();
                hashMap.put("JobStatus", "COMPLETED");
                jedis.hset(entryId.toString(), hashMap);

                Map<String, String> keys = jedis.hgetAll(entryId.toString());
                System.out.println("Job Information Provided! " + keys);

                System.out.println("Entry ID: " + entryId);

                // You can also get other fields
                System.out.println("Fields: " + streamEntry.getFields());
            }
        }
    }
}
