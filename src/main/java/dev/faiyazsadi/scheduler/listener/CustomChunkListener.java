package dev.faiyazsadi.scheduler.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class CustomChunkListener implements ChunkListener {

    private final CustomJobExecutionListener customJobExecutionListener;
    private final JedisPool pool;

    private static final ConcurrentHashMap<Long, Long> processedChunks = new ConcurrentHashMap<>();

    @Override
    public void afterChunk(ChunkContext chunkContext) {

        StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
        JobExecution jobExecution = stepExecution.getJobExecution();
        Long jobExecutionId = jobExecution.getId();
        String hashKey = jobExecution.getJobParameters().getString("jobEntryID");

        // Increment the count of processed chunks for the specific jobExecutionId
        processedChunks.merge(jobExecutionId, 1L, Long::sum);

        Long datasetSize = customJobExecutionListener.getDatasetLen(jobExecutionId);

        Long chunkSize = 100L;
        Double reqChunk = Double.valueOf(datasetSize) / chunkSize;
        Long processedChunk = processedChunks.get(jobExecutionId);

        Double percentage = (Double.valueOf(processedChunk) / reqChunk) * 100;
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(hashKey, "percentage", percentage.toString());
        }
    }

    public Long getProcessedChunkCount(Long jobExecutionId) {
        return processedChunks.get(jobExecutionId);
    }
}
