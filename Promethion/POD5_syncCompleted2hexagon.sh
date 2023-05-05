#!/bin/bash
#Purpose = sync all other miscellaneous files from Promethion machine to mnt/seq network folder 
#Start
# run this and check error and email error
# */30 * * * * run-one rsync -avu --include="*/" --include="*.fast5" --include="*.fastq.gz" --exclude="*" \
# /data/TEST_CRON/ /mnt/seq/gridion/MinION_testing_data/ >> /data/TEST_CRON/rsync.log 2>&1

TEST="${TEST:-$1}"

#Variables
SOURCE="/data/T2T_project/data"
DESTINATION="/mnt/hexagon-scratch/Instrument_Data_Share/T2T_project"
LOGFILE="/data/T2T_project/rsync_COMPLETED.log"
ERROR="/data/T2T_project/rsync_COMPLETED.ERROR"

# MongoDb 
MONGO_EXEC="/data/T2T_project/mongo/bin/mongo"
DB_LOG="/data/T2T_project/mongo.log"
DB_USER=promappuser
MONGODB_PW="q^a#W3LTjq?t^kwB"
DB_IP=54.251.49.171:8080/promethion
DB_NAME=promethion

# FILE Type
SYNC_FILE_TYPE="*.pod5"
SYNC_FASTQ="*.fastq.gz"

find ${SOURCE} -iname "final_summary_*.txt" | while read SUMMARY_FILE ; do
	# echo "${SUMMARY_FILE} "
	# eg. /data/T2T_project/data/ultra_long/ON002-DNA-R00497_done/WHB13034-T1/20230220_0837_3E_PAM76498_47345b2a/final_summary_PAM76498_47345b2a_557aaab0.txt
	IFS='/' read -ra ARRAY <<< "${SUMMARY_FILE}"
	MODE=${ARRAY[-5]}
	RUN=${ARRAY[-4]}
	SAMPLE=${ARRAY[-3]}
	FLOWCELL=${ARRAY[-2]}
	FILE=${ARRAY[-1]}
	MUX_FULL_PATH="${RUN}/${SAMPLE}/${FLOWCELL}"
		
	echo -e `date +"%F\t%T\t"`"Processing: ${FILE} ( MODE=${MODE}, RUN: ${RUN}, SAMPLE: ${SAMPLE}, FLOWCELL: ${FLOWCELL}) " >>${ERROR}
	
	SYNC_SOURCE=${SOURCE}/${MODE}/${MUX_FULL_PATH}/
	SYNC_DEST=${DESTINATION}/${MODE}/${MUX_FULL_PATH}/
		
	# Check seq_summary for pod5 count
	POD5_COUNT_SEQSUMMARY=$(find ${SYNC_SOURCE} -iname "sequencing_summary_*.txt" -exec cut -f 3 {} \; | sed '1d' | sort | uniq | wc -l)
	POD5_COUNT_HEXAGON=$(find ${SYNC_DEST} -iname ${SYNC_FILE_TYPE} | wc -l)
	# echo ${POD5_COUNT_PROM}
	
	if [[ "${POD5_COUNT_SEQSUMMARY}" == "${POD5_COUNT_HEXAGON}" ]]; then
		# rsync ONLY 1 MUX folder to hexagon
		# NOT yet: 
		run-one rsync -avu --remove-source-files \
		--exclude=${SYNC_FILE_TYPE} --exclude=${SYNC_FASTQ} \
		${SYNC_SOURCE} ${SYNC_DEST} \
		>> ${LOGFILE} 2>>${ERROR}
		SUCCESS=$?
		if [[ ${SUCCESS} -eq 0 ]]; then
	
			STATUS="To_Basecall"
					
			EVAL='db.T2T_project_2023.insert({"run_id": "'"${RUN}"'" , "SAMPLE": "'"${SAMPLE}"'" , "FLOWCELL": "'"${FLOWCELL}"'" , "mux_full_path": "'"${MUX_FULL_PATH}"'" , "pod5_hexagon_count": "'"${POD5_COUNT_HEXAGON}"'" , "pod5_seqSummary_count": "'"${POD5_COUNT_SEQSUMMARY}"'" , "Status": "'"${STATUS}"'" , "Mode": "'"${MODE}"'" });'
			echo "insertSql: $EVAL" >> ${DB_LOG}
			result=$(echo $EVAL | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
			echo "outcome: $result" >> ${DB_LOG}
			echo -e `date +"%F\t%T\t"`"COMPLETED: ${FILE} ( RUN: ${RUN}, SAMPLE: ${SAMPLE}, FLOWCELL: ${FLOWCELL}) " >>${ERROR}
		else
			# rsync failed!
			echo -e `date +"%F\t%T\t"`"[ERROR] Rsync ${SYNC_SOURCE}" >>${ERROR}
			exit 1
		fi
	else
		echo -e `date +"%F\t%T\t"`"[INCOMPLETE] HEXAGON_POD5_count: ${POD5_COUNT_HEXAGON} ; SEQ_SUMMAR_POD5_COUNT: ${POD5_COUNT_SEQSUMMARY}" >>${ERROR} 
	fi
done
#END