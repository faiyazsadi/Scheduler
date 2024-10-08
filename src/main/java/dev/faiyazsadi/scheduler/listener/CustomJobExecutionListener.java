package dev.faiyazsadi.scheduler.listener;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Component
public class CustomJobExecutionListener implements JobExecutionListener {

    private static final ConcurrentHashMap<Long, Long> datasetLen = new ConcurrentHashMap<>();
    private final JedisPool jedisPool;
    private final long TTL = 10; // seconds

    @Override
    public void afterJob(JobExecution jobExecution) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = jobExecution.getJobParameters().getString("jobEntryID");
            jedis.expire(hashKey, TTL);
        }
    }

    @SneakyThrows
    @Override
    public void beforeJob(JobExecution jobExecution) {
        String fileName = jobExecution.getJobParameters().getString("fileName");
        Resource resource = new ClassPathResource(Objects.requireNonNullElse(fileName, "customers-10k.csv"));

        if (!resource.exists()) {
            throw new FileNotFoundException(fileName);
        }

        long lineCount = 0L;
        Path filePath = Paths.get(resource.getURI());
        try (Stream<String> stream = Files.lines(filePath, StandardCharsets.UTF_8)) {
            lineCount = stream.count();
            datasetLen.put(jobExecution.getJobId(), lineCount);
        }
    }

    public Long getDatasetLen(Long jobId) {
        if(datasetLen.containsKey(jobId)) {
            return datasetLen.get(jobId);
        } else {
            throw new RuntimeException("Invalid job id: " + jobId);
        }
    }
}