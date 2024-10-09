#!/bin/bash

# Redis server host and port
REDIS_HOST="localhost"
REDIS_PORT="6379"

# Stream name
STREAM_NAME="JOBS"

# Number of jobs to add
NO_OF_JOBS=10

echo "Stream '$STREAM_NAME' does not exist. Adding entries..."

# Loop to add entries
for ((i=1; i<=NO_OF_JOBS; i++))
do
    # Define fields for each entry
    JOB_NAME="job-$i"
    PROJECT_NAME="project-$i"
    FILE_NAME="customers-1m.csv"

    # Add entry to the Redis stream
    redis-cli -h $REDIS_HOST -p $REDIS_PORT XADD $STREAM_NAME "*" \
        jobName "$JOB_NAME" \
        projectName "$PROJECT_NAME" \
        fileName "$FILE_NAME"

    echo "Added entry $i: jobName=$JOB_NAME, projectName=$PROJECT_NAME, fileName=$FILE_NAME"
done

echo "Finished adding $NO_OF_JOBS jobs to the stream '$STREAM_NAME'."
