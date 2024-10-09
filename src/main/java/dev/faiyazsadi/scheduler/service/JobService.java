package dev.faiyazsadi.scheduler.service;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

@Service
public class JobService {
    private final JobLauncher jobLauncher;
    private final Job job;
    private final String STREAM_NAME;
    private final String GROUP_NAME;

    public JobService(Job job,
                      @Qualifier("consumerGroupName") String STREAM_NAME,
                      @Qualifier("consumerGroupName") String GROUP_NAME,
                      @Qualifier("CustomJobLauncher") JobLauncher jobLauncher) {
        this.job = job;
        this.STREAM_NAME = STREAM_NAME;
        this.GROUP_NAME = GROUP_NAME;
        this.jobLauncher = jobLauncher;
    }

    public void runJob(Jedis jedis, List<Map.Entry<String, List<StreamEntry>>> message) throws FileNotFoundException {
        if (message == null) {
            return;
        }

        // Process the messages
        for (Map.Entry<String, List<StreamEntry>> entry : message) {
            List<StreamEntry> streamEntries = entry.getValue();

            for (StreamEntry streamEntry : streamEntries) {
                StreamEntryID entryId = streamEntry.getID();
                jedis.hset(entryId.toString(), "JobStatus", "NOT INITIALIZED");

                String fileName = streamEntry.getFields().get("fileName");

                Resource resource = new ClassPathResource(fileName);
                if(!resource.exists()) {
                    throw new FileNotFoundException("File " + fileName + " Does Not Exist!");
                }

                JobParameters jobParameters = new JobParametersBuilder()
                    .addString("fileName", fileName)
                    .addLong("startAt", System.currentTimeMillis())
                    .addString("jobEntryID", entryId.toString()).toJobParameters();

                try {
                    JobExecution jobExecution = jobLauncher.run(job, jobParameters);

                    StreamEntryID entryID = streamEntry.getID();
                    jedis.xack(STREAM_NAME, GROUP_NAME, entryId);

                    jedis.hset(entryID.toString(), "BatchJobId", jobExecution.getJobId().toString());
                    jedis.hset(entryID.toString(), "JobStatus", jobExecution.getStatus().toString());

                } catch (JobExecutionAlreadyRunningException
                         | JobRestartException
                         | JobInstanceAlreadyCompleteException
                         | JobParametersInvalidException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
