#!/bin/bash
# Purpose = sync files from Promethion machine to hexagon-scratch 

# Variables
SOURCE="/data/T2T_project/data"
DESTINATION="/mnt/hexagon-scratch/Instrument_Data_Share/T2T_project"
LOGFILE="/data/T2T_project/rsync.log"
ERROR="/data/T2T_project/rsync.error.log"

# MongoDb 
MONGO_EXEC="/data/T2T_project/mongo/bin/mongo"
DB_LOG="/data/T2T_project/mongo.log"
DB_USER=promappuser
MONGODB_PW='q^a#W3LTjq?t^kwB'
DB_IP=54.251.49.171:8080/promethion
DB_NAME=promethion
DB="T2T_test"

STATUS="Sync_pod5"

# FILE Type
SYNC_FILE_TYPE="*.pod5"
SYNC_FASTQ="*.fastq.gz"

# 
run-one rsync -avu --remove-source-files \
--include="*/" --include="*.pod5" --include="*.fastq.gz" --exclude="*" \
${SOURCE}/ ${DESTINATION} \
>> $LOGFILE 2>>${ERROR}
SUCCESS=$?

#END