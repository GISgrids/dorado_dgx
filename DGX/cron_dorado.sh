#!/bin/bash

#----------------------------------------------------
# FOLDERS VARIABLE
SOURCE="/mnt/hexagon-scratch/Instrument_Data_Share/T2T_project"
PROJECT_DIR="/raid/scratch/lizh/T2T_project"
INSTRUMENT_DATA="/mnt/hexagon-scratch/Instrument_Data_Share/T2T_project"
HEXAGON="/mnt/hexagon/lizh/T2T_project"

# DGX data dir
DGX_DATA="${PROJECT_DIR}/data"
DGX_ID="DGX02"

# Backup locale
# S3_dir="/mnt/hexagon-scratch/lizh/methy_test/guppy6_result"
S3_dir=${HEXAGON}

# Log files
LOGFILE="${PROJECT_DIR}/Dorado_cron.log"
ERROR="${PROJECT_DIR}/Dorado_cron.ERROR"

# MongoDb 
MONGO_EXEC="${PROJECT_DIR}/mongo/bin/mongo"
DB_LOG="${PROJECT_DIR}/mongo.log"
DB_USER=promappuser
MONGODB_PW="q^a#W3LTjq?t^kwB"
DB_IP="54.251.49.171:8080/promethion"
DB_NAME="promethion"
LD_LIBRARY_PATH=/raid/scratch/lizh/T2T_project/mongo/openssl/openssl_1.0.2/usr/lib/x86_64-linux-gnu

# DOCKER variable
DOCKER="cca4ce53a02d"
GUPPY_VERSION="Dorado_0.2.1"

# FILE Type
SYNC_FILE_TYPE="*.pod5"

# Dorado variable
DEVICE="cuda:0,1,2,3" # 
MODEL_DIR="${PROJECT_DIR}/dorado_model"
MODEL_UL="dna_r10.4.1_e8.2_400bps_sup@v4.1.0"
MODEL_DUPLEX="dna_r10.4.1_e8.2_400bps_hac@v4.1.0"
ENV="duplex_tools_latest"
THREADS=40

# Max Dorado run in a row
MAX_RUN=10
#-----------------------------------------------------

lockdir=${PROJECT_DIR}/tmp/dorado_cron.lock
declare -i i=0
while [ $i -lt ${MAX_RUN} ]
do 
	#echo -e `date +"%F\t%T\t"`"Run $i" >>${ERROR}

	if mkdir -- "$lockdir" ; then
		# echo -e `date +"%F\t%T\t"`"[INFO] Successfully acquired lock on Run $i" >>${ERROR}
		
		# Set to NA to assume NOT RERUN
		STAGE="NA"
		SUCCESS_STAGE=1
		export LD_LIBRARY_PATH
		source activate $ENV
		cd ${PROJECT_DIR}
		
		# Check for MUX_ID with Status "RERUN" from mongoDB -> FAILED Dorado 
		EVAL='db.T2T_project_2023.findOne({"Status": "RERUN"})._id ; '
		echo "querySql: $EVAL" >> ${DB_LOG}
		DOC_ID=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
		SUCCESS_DOC=$?
		
		EVAL='db.T2T_project_2023.findOne('"${DOC_ID}"').DGX_ID ; '
		echo "querySql: $EVAL" >> ${DB_LOG}
		DGX_RERUN=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
				
		if [[ ${SUCCESS_DOC} -eq 0 && "${DGX_RERUN}" == "${DGX_ID}" ]]; then
			# RERUN Routine and on correct DGX machine
			echo -e `date +"%F\t%T\t"`"[INFO] RERUN ${DOC_ID} on ${DGX_RERUN}" >>${ERROR}
			
			# Find which stage to rerun
			EVAL='db.T2T_project_2023.findOne('"${DOC_ID}"').Stage ; '
			echo "querySql: $EVAL" >> ${DB_LOG}
			STAGE=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
			SUCCESS_STAGE=$?
		else
			# Check for MUX_ID with Status "To_Basecall" from mongoDB
			EVAL='db.T2T_project_2023.findOne({"Status": "To_Basecall"})._id ; '
			echo "querySql: $EVAL" >> ${DB_LOG}
			DOC_ID=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
			SUCCESS_DOC=$?
		fi
		
		# Get mux_full_path
		EVAL='db.T2T_project_2023.findOne('"${DOC_ID}"').mux_full_path ; '
		echo "querySql: $EVAL" >> ${DB_LOG}
		MUX_FULL_PATH=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
		# echo "${DOC_ID} , ${MUX_FULL_PATH}"
		SUCCESS_PATH=$?
		
		# Get basecall MODE
		# eg duplex , porec , ultra_long , test1
		EVAL='db.T2T_project_2023.findOne('"${DOC_ID}"').Mode ; '
		echo "querySql: $EVAL" >> ${DB_LOG}
		MODE=$(echo "$EVAL" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
		# echo "${DOC_ID} , ${MUX_FULL_PATH}"
		SUCCESS_MODE=$?
		
		### CREATE folder in DGX for output
		# MUX_FULL_PATH example: ON002-DNA-R00141/WHB12183-T1/20210208_0141_1G_PAG51949_51a655a0/
		IFS='/' read -ra ARRAY <<< "${MUX_FULL_PATH}"
		RUN=${ARRAY[-3]}
		SAMPLE=${ARRAY[-2]}
		INDEX=${ARRAY[-1]}

		# Folder
		SYNC_SOURCE=${SOURCE}/${MODE}/${RUN}/${SAMPLE}/${INDEX}
		SYNC_DEST=${DGX_DATA}/${MODE}/${RUN}/${SAMPLE}/${INDEX}
		BASECALL_FOLDER="${DGX_DATA}/${MODE}/${RUN}/${SAMPLE}/${INDEX}/pod5_combined"
		BASECALL_OUTPUT="${DGX_DATA}/${MODE}/${RUN}/${SAMPLE}/${INDEX}/Dorado_${MODE}"		
		
		# Create folder
		# [ ! -d ${SYNC_DEST} ] && mkdir -p ${SYNC_DEST} && echo -e `date +"%F\t%T\t"`"[INFO] Created ${SYNC_DEST}" >>${ERROR}
		[ ! -d ${BASECALL_FOLDER} ] && mkdir -p ${BASECALL_FOLDER} >> ${LOGFILE} 2>>${ERROR}
		[ ! -d ${BASECALL_OUTPUT} ] && mkdir -p ${BASECALL_OUTPUT} >> ${LOGFILE} 2>>${ERROR}
		
		# flowcell-level log
		FC_LOGFILE="${BASECALL_OUTPUT}/${RUN}.flowcell.log"
		FC_ERROR="${BASECALL_OUTPUT}/${RUN}.flowcell.ERROR"
				
		if [[ ${SUCCESS_DOC} -eq 0 && ${SUCCESS_PATH} -eq 0 && ${SUCCESS_MODE} -eq 0  && ${SUCCESS_STAGE} -eq 0 ]]; then
			echo -e `date +"%F\t%T\t"`"[INFO] RERUN STAGE: ${STAGE} on MUX: ${RUN}/${SAMPLE}/${INDEX}" >>${ERROR}

			# Update MUX_ID Status -> "Basecalling"
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "Basecalling"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
			echo "outcome: $result" >> ${DB_LOG}
			
		elif [[ ${SUCCESS_DOC} -eq 0 && ${SUCCESS_PATH} -eq 0 && ${SUCCESS_MODE} -eq 0 && ${SUCCESS_STAGE} -ne 0 ]]; then
			echo -e `date +"%F\t%T\t"`"[INFO] Processing: ${RUN}/${SAMPLE}/${INDEX}" >>${ERROR}

			# Update MUX_ID Status -> "Basecalling"
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "Basecalling"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet)
			echo "outcome: $result" >> ${DB_LOG}
										
			STAGE="RSYNC_TO_DGX"

		else
			# echo -e `date +"%F\t%T\t"`"[INFO] Query to MonogDB failed...Nothing to do... " >>${ERROR}
			rm -rf -- "$lockdir"
			exit 0
		fi  
		
		# Confirm got things to do, set DGX_ID
		# Update MongoDB on machine running basecall
		UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"DGX_ID": "'"${DGX_ID}"'"} } ) ; '
		echo "UpdateSql: $UPDATE" >> ${DB_LOG}
		result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
		echo "outcome: $result" >> ${DB_LOG}

		if [[ "${STAGE}" == "RSYNC_TO_DGX" ]]; then
			echo -e `date +"%F\t%T\t"`"[START] Sync data from ${SYNC_SOURCE} " >>${ERROR}
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			# Only sync POD5 files - from both pass and fail folder
			
			# 1. sync - pod5_pass
			run-one rsync -avu --include="*/" --include=${SYNC_FILE_TYPE} --exclude="*" ${SYNC_SOURCE}/pod5_pass/ ${SYNC_DEST}/pod5_combined >> ${FC_LOGFILE} 2>>${FC_ERROR}
			SUCCESS_SYNC_PASS=$?
			# 2. sync - pod5_fail
			run-one rsync -avu --include="*/" --include=${SYNC_FILE_TYPE} --exclude="*" ${SYNC_SOURCE}/pod5_fail/ ${SYNC_DEST}/pod5_combined >> ${FC_LOGFILE} 2>>${FC_ERROR}
			SUCCESS_SYNC_FAIL=$?
			#echo "SUCCESS_SYNC is $SUCCESS_SYNC"
			
			if [[ "${SUCCESS_SYNC_PASS}" -eq 0 && "${SUCCESS_SYNC_FAIL}" -eq 0 ]]; then
				echo -e `date +"%F\t%T\t"`"[END] Sync data to ${SYNC_DEST}" >>${ERROR}
				if [[ "${MODE}" == "ultra_long" || "${MODE}" == "porec" ]]; then
					STAGE="DORADO"
				elif [[ "${MODE}" == "duplex" ]]; then
					# 3 Stages serial run
					# https://github.com/nanoporetech/duplex-tools#usage-with-dorado-recommended
					# stage1: "DORADO_DUPLEX_Round1"
					# stage2a: "DUPLEX_TOOLS_A"
					# stage2b: "DUPLEX_TOOLS_B"
					# stage3a: "DORADO_DUPLEX_Round2A"
					# stage3b: "DORADO_DUPLEX_Round2B"
					STAGE="DORADO_DUPLEX_Round1"
				fi
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] REMOVE LOCK. FAILED Sync data to ${SYNC_DEST}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}
				# Remove lock
				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DORADO" ]]; then
		
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
				
			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}			
			docker run --gpus all --name ${GUPPY_VERSION} --rm \
			-v ${PROJECT_DIR}/:${PROJECT_DIR}/ \
			-v ${INSTRUMENT_DATA}/:/${INSTRUMENT_DATA}/ \
			-v ${HEXAGON}/:${HEXAGON}/ \
			-u $(id -u ${USER}):$(id -g ${USER}) \
			nanoporetech/dorado /bin/bash -c \
			"dorado basecaller -x ${DEVICE} ${MODEL_DIR}/${MODEL_UL} ${BASECALL_FOLDER}/ > ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam 2>>${FC_ERROR}" 
			SUCCESS_BASECALL=$?
			
			if [ ${SUCCESS_BASECALL} -eq 0 ]; then
				# echo Round1 BC succeeded
				echo -e `date +"%F\t%T\t"`"[START] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				samtools view --threads ${THREADS} --bam -Sh -O BAM -o ${BASECALL_OUTPUT}/${RUN}.${MODE}.bam ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam >> ${FC_LOGFILE} 2>>${FC_ERROR}
				SUCCESS_SAMTOOLS=$?
				
				if [ ${SUCCESS_SAMTOOLS} -eq 0 ]; then
					rm ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam
					samtools index ${BASECALL_OUTPUT}/${RUN}.${MODE}.bam >> ${FC_LOGFILE} 2>>${FC_ERROR}
					echo -e `date +"%F\t%T\t"`"[END] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				else
					echo -e `date +"%F\t%T\t"`"[ERROR] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam" >>${ERROR}
					rm -rf -- "$lockdir"
					exit 2
				fi
				
				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update to next stage
				STAGE="DATA_BACKUP"
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				# remove lock
				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DORADO_DUPLEX_Round1" ]]; then

			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; MODEL=${MODEL_DUPLEX} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}			
			docker run --gpus all --name ${GUPPY_VERSION} --rm \
			-v ${PROJECT_DIR}/:${PROJECT_DIR}/ \
			-v ${INSTRUMENT_DATA}/:/${INSTRUMENT_DATA}/ \
			-v ${HEXAGON}/:${HEXAGON}/ \
			-u $(id -u ${USER}):$(id -g ${USER}) \
			nanoporetech/dorado /bin/bash -c \
			"dorado basecaller --emit-moves -x ${DEVICE} ${MODEL_DIR}/${MODEL_DUPLEX} ${BASECALL_FOLDER}/ > ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam 2>>${FC_ERROR}"
			SUCCESS_BASECALL_RD1=$?
			
			if [ ${SUCCESS_BASECALL_RD1} -eq 0 ]; then
				# echo Round1 BC succeeded
				echo -e `date +"%F\t%T\t"`"[START] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				samtools view --threads ${THREADS} --bam -Sh -O BAM -o ${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam >> ${FC_LOGFILE} 2>>${FC_ERROR}
				SUCCESS_SAMTOOLS=$?
				
				if [ ${SUCCESS_SAMTOOLS} -eq 0 ]; then
					rm ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam
					samtools index ${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam >> ${FC_LOGFILE} 2>>${FC_ERROR}
					echo -e `date +"%F\t%T\t"`"[END] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				else
					echo -e `date +"%F\t%T\t"`"[ERROR] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam" >>${ERROR}
					rm -rf -- "$lockdir"
					exit 2
				fi
				
				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} ; MODEL=${MODEL_DUPLEX} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				
				# update to next stage
				STAGE="DUPLEX_TOOLS_A"
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; MODEL=${MODEL_DUPLEX} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}
				
				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DUPLEX_TOOLS_A" ]]; then						
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			# Run duplex_tools pair on bam

			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
			# stderr to stdout for grep; NOT output continuous update eg "4743236it [35:43, 2231.34it/s]"
			duplex_tools pair --output_dir ${BASECALL_OUTPUT}/pairs_from_bam --prefix ${RUN} --threads ${THREADS} ${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam >> ${FC_LOGFILE} 2>>${FC_ERROR}
			SUCCESS_DUPLEX_TOOLS_A=$?
			if [ ${SUCCESS_DUPLEX_TOOLS_A} -eq 0 ]; then
				# echo Duplex_tools succeeded
				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
				STAGE="DUPLEX_TOOLS_B"
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DUPLEX_TOOLS_B" ]]; then						
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			# Run duplex_tools pair on bam

			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
			# stderr to stdout for grep; NOT output continuous update from 2 patterns
			# eg1 "4743236it [35:43, 2231.34it/s]"
			# eg2 [07:38:17 - SplitPairs] Split/Processed reads:44359/4750000 (0.93%)
			duplex_tools split_pairs --threads ${THREADS} ${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam ${BASECALL_FOLDER} ${BASECALL_OUTPUT}/pod5s_splitduplex >> ${FC_LOGFILE} 2>>${FC_ERROR}
			SUCCESS_DUPLEX_TOOLS_B=$?
			if [ ${SUCCESS_DUPLEX_TOOLS_B} -eq 0 ]; then
				# echo Duplex_tools succeeded
				cat ${BASECALL_OUTPUT}/pod5s_splitduplex/*_pair_ids.txt > ${BASECALL_OUTPUT}/pod5s_splitduplex/split_duplex_pair_ids.txt
				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
				STAGE="DORADO_DUPLEX_Round2A"
				
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; BAM=${BASECALL_OUTPUT}/${RUN}.unmapped_reads_with_moves.bam" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DORADO_DUPLEX_Round2A" ]]; then		
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
			docker run --gpus all --name ${GUPPY_VERSION} --rm \
			-v ${PROJECT_DIR}/:${PROJECT_DIR}/ \
			-v ${INSTRUMENT_DATA}/:/${INSTRUMENT_DATA}/ \
			-v ${HEXAGON}/:${HEXAGON}/ \
			-u $(id -u ${USER}):$(id -g ${USER}) \
			nanoporetech/dorado /bin/bash -c \
			"dorado duplex -x ${DEVICE} --pairs ${BASECALL_OUTPUT}/pairs_from_bam/pair_ids_filtered.txt ${MODEL_DIR}/${MODEL_UL} ${BASECALL_FOLDER}/ > ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam 2>>${FC_ERROR}" 
			SUCCESS_BASECALL_RD2A=$?
			
			if [ ${SUCCESS_BASECALL_RD2A} -eq 0 ]; then
				# echo Round1 BC succeeded
				echo -e `date +"%F\t%T\t"`"[START] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				samtools view --threads ${THREADS} --bam -Sh -O BAM -o ${BASECALL_OUTPUT}/${RUN}.${STAGE}.bam ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam >> ${FC_LOGFILE} 2>>${FC_ERROR}
				SUCCESS_SAMTOOLS=$?
				
				if [ ${SUCCESS_SAMTOOLS} -eq 0 ]; then
					rm ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam
					samtools index ${BASECALL_OUTPUT}/${RUN}.${STAGE}.bam >> ${FC_LOGFILE} 2>>${FC_ERROR}
					echo -e `date +"%F\t%T\t"`"[END] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				else
					echo -e `date +"%F\t%T\t"`"[ERROR] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam" >>${ERROR}
					rm -rf -- "$lockdir"
					exit 2
				fi

				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update to new stage
				STAGE="DORADO_DUPLEX_Round2B"
				
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DORADO_DUPLEX_Round2B" ]]; then		
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			echo -e `date +"%F\t%T\t"`"[START] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
			docker run --gpus all --name ${GUPPY_VERSION} --rm \
			-v ${PROJECT_DIR}/:${PROJECT_DIR}/ \
			-v ${INSTRUMENT_DATA}/:/${INSTRUMENT_DATA}/ \
			-v ${HEXAGON}/:${HEXAGON}/ \
			-u $(id -u ${USER}):$(id -g ${USER}) \
			nanoporetech/dorado /bin/bash -c \
			"dorado duplex -x ${DEVICE} --pairs ${BASECALL_OUTPUT}/pod5s_splitduplex/split_duplex_pair_ids.txt ${MODEL_DIR}/${MODEL_UL} ${BASECALL_OUTPUT}/pod5s_splitduplex/ > ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam 2>>${FC_ERROR}" 
			SUCCESS_BASECALL_RD2B=$?
			
			if [ ${SUCCESS_BASECALL_RD2B} -eq 0 ]; then
				echo -e `date +"%F\t%T\t"`"[START] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				samtools view --threads ${THREADS} --bam -Sh -O BAM -o ${BASECALL_OUTPUT}/${RUN}.${STAGE}.bam ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam >> ${FC_LOGFILE} 2>>${FC_ERROR}
				SUCCESS_SAMTOOLS=$?
				
				if [ ${SUCCESS_SAMTOOLS} -eq 0 ]; then
					rm ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam
					samtools index ${BASECALL_OUTPUT}/${RUN}.${STAGE}.bam >> ${FC_LOGFILE} 2>>${FC_ERROR}
					echo -e `date +"%F\t%T\t"`"[END] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam " >>${ERROR}
				else
					echo -e `date +"%F\t%T\t"`"[ERROR] SAM2BAM and index: ${BASECALL_OUTPUT}/${RUN}.${STAGE}.sam" >>${ERROR}
					rm -rf -- "$lockdir"
					exit 2
				fi
				
				echo -e `date +"%F\t%T\t"`"[END] ${STAGE} & REMOVE LOCK ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				
				rm -rf -- "$lockdir"
				# Update to new stage
				STAGE="DATA_BACKUP"
				
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] ${STAGE} ; MODEL=${MODEL_UL} ; BASECALL_FOLDER=${BASECALL_FOLDER}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				rm -rf -- "$lockdir"
				exit 2
			fi
		fi
		
		if [[ "${STAGE}" == "DATA_BACKUP" ]]; then
			
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Stage": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			# Run quality metric collection
			mapfile -d $'\0' arr < <(find ${BASECALL_OUTPUT} -name "*.bam" -print0)
			for bam in "${arr[@]}"; do
				python bin/get_read_length_quality.py --input_bam ${bam} --output_dir ${BASECALL_OUTPUT}
			done

			# Sync basecall result back to hexagon				
			S3_source="${BASECALL_OUTPUT}"
			S3_dest="${S3_dir}/${MODE}/${RUN}/${SAMPLE}/${INDEX}/Dorado_${MODE}"
			[ ! -d ${S3_dest} ] && mkdir -p ${S3_dest} && echo "Created ${S3_dest}"
			
			echo -e `date +"%F\t%T\t"`"[START] Rsync from ${S3_source}" >>${ERROR} 
			run-one rsync -avu -q ${S3_source}/ ${S3_dest}/ >> ${FC_LOGFILE} 2>>${FC_ERROR}
			SUCCESS_SYNC=$?
							
			if [ ${SUCCESS_SYNC} -eq 0 ]; then

				echo -e `date +"%F\t%T\t"`"[END] Rsync to ${S3_dest}." >>${ERROR}
				
				# Update to new stage
				STAGE="COMPLETE"
			else
				echo -e `date +"%F\t%T\t"`"[ERROR] Rsync to ${S3_dest}" >>${ERROR}
				# Update Status
				UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "RERUN"} } ) ; '
				echo "UpdateSql: $UPDATE" >> ${DB_LOG}
				result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
				echo "outcome: $result" >> ${DB_LOG}

				rm -rf -- "$lockdir"
				exit 2				
			fi
		fi
		
		if [[ "${STAGE}" == "COMPLETE" ]]; then
			# Update Stage in MongoDB
			UPDATE='db.T2T_project_2023.updateOne( {"_id": '"${DOC_ID}"' } , { $set:{"Status": "'"${STAGE}"'"} } ) ; '
			echo "UpdateSql: $UPDATE" >> ${DB_LOG}
			result=$(echo "$UPDATE" | ${MONGO_EXEC} -u ${DB_USER} -p "${MONGODB_PW}" ${DB_IP} --authenticationDatabase ${DB_NAME} --quiet )
			echo "outcome: $result" >> ${DB_LOG}
			
			echo -e `date +"%F\t%T\t"`"[COMPLETE] ${MODE}/${RUN}/${SAMPLE}/${INDEX}." >>${ERROR}
			# Remove folder
			rm -rf ${BASECALL_FOLDER}
			rm -rf ${BASECALL_OUTPUT}
			find ${DGX_DATA}/${MODE}/${RUN} -depth -exec rmdir {} \; 
		fi
		# remove directory when script finishes
		trap 'rm -rf -- "$lockdir"; echo " Remove lock2"' 0    
		i+=1
	else
		printf >&2 "Run $i cannot acquire lock, giving up on %s\n $lockdir"
		exit 0
	fi
done
