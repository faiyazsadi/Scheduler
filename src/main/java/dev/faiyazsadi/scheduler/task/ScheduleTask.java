package dev.faiyazsadi.scheduler.task;

import dev.faiyazsadi.scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class ScheduleTask {
    private final JobService jobService;
    private final JedisPool pool;
    final int rateSeconds = 2;

    @Scheduled(fixedRate = rateSeconds * 1000, initialDelay = 1000)
    public void runTask() {
        System.out.println("Task is running every " + rateSeconds + " seconds: " + System.currentTimeMillis());

        String STREAM_NAME = "JOBS";
        String GROUP_NAME  = "EXECUTORS";

        int NO_OF_CONSUMERS = 2;
        List<String> CONSUMERS = new ArrayList<>();
        for (int i = 1; i <= NO_OF_CONSUMERS; i++) {
            String consumer = "EXECUTOR-" + i;
            CONSUMERS.add(consumer);
        }

        try (Jedis jedis = pool.getResource()) {
            for (String CONSUMER : CONSUMERS) {
                List<Map.Entry<String, List<StreamEntry>>> message = readMessage(jedis, GROUP_NAME, CONSUMER, STREAM_NAME);
                printMessageInfo(message, jedis);
                jobService.runJob(jedis, message);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Map.Entry<String, List<StreamEntry>>> readMessage(Jedis jedis, String GROUP_NAME, String CONSUMER, String STREAM_NAME) {
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

    private void printMessageInfo(List<Map.Entry<String, List<StreamEntry>>> message, Jedis jedis) {

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

                System.out.println("---------------------------------------------------------");
                System.out.println("---------------------------------------------------------");
            }
        }
    }
}
