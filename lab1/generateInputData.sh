#!/bin/bash
if [[ $# -lt 2 ]] ; then
    echo 'Usage: ./generateInputData.sh log_entries_count output_file_name'
    exit 1
fi

if [[ $# -gt 2 ]] ; then
    echo 'Too many parameters'
    exit 1
fi

START_TIME=$(date --iso-8601=seconds)
LAST_LOG_TIME=$START_TIME
LOG_ENTRIES_COUNT=$1
OUTPUT_FILE_NAME=$2

# since there is no metadata (including severity level) in logs stored in disk
# assume the logs have the format below
# [timestamp] [severity level] [message]
SEVERITY_LEVELS=("emerg"
    "panic"
    "alert"
    "crit"
    "err"
    "error"
    "warn"
    "warning"
    "notice"
    "info"
    "debug"
)

# message doesn't mean something - it can be repeatable
MESSAGE="application: bla bla bla"

rm -rf input
mkdir input

for ((i=1;i<=LOG_ENTRIES_COUNT;i++))
do
  SEVERITY_LEVEL="${SEVERITY_LEVELS[$((RANDOM % ${#SEVERITY_LEVELS[*]}))]}"

  LAST_LOG_TIME=$(date -d "$LAST_LOG_TIME +second" --iso-8601=seconds)

  RESULT="$(date -d $LAST_LOG_TIME +%F\ %T) [$SEVERITY_LEVEL]$(printf '\t')$MESSAGE"
   echo "$RESULT"
   echo "$RESULT" >> input/"$OUTPUT_FILE_NAME"
done
