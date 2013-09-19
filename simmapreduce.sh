#!/bin/bash

function log
{
  echo "`date '+%y/%m/%d %H:%M'` $1 $2: $3"
}

function preamble
{
  log "WARN" "snappy.LoadSnappy" "Snappy native library is available"
  log "INFO" "util.NativeCodeLoader" "Loaded the native-hadoop library"
  log "INFO" "snappy.LoadSnappy" "Snappy native library loaded"
  sleep 3s
  log "INFO" "mapred.FileInputFormat" "Total input paths to process : $paths"
  sleep 5s
  log "INFO" "mapred.JobClient" "Running job: $job_id"
}

function mapreduce
{
  map_percentage=0
  red_percentage=0
  while ((map_percentage + red_percentage <= 200 ))
  do
    wait_random=$RANDOM
    let "wait_random %=wait_amount"
    wait_time=`expr $wait_amount + $wait_random`
    if [ $red_percentage -gt 98 ]; then
      let "wait_time *=5"
    fi
    log "INFO" "mapred.JobClient" " map ${map_percentage}% reduce ${red_percentage}%"
    sleep ${wait_time}s
    if [ $map_percentage -lt 100 ]; then
      ((map_percentage++))
    else
      ((red_percentage++))
    fi
  done
}

function log_counter
{
  meaningless_big_data_number=${2:-`expr $RANDOM \* $RANDOM \* $RANDOM`}
  log "INFO" "mapred.JobClient" "     ${1}=${meaningless_big_data_number}"
}

function counters
{
  log "INFO" "mapred.JobClient" "Job complete: ${job_id}"
  log "INFO" "mapred.JobClient" " Counters: 25"
  log "INFO" "mapred.JobClient" "   Job Counters "
  log_counter "Launched reduce tasks" "10"
  log_counter "SLOTS_MILLIS_MAPS" "35359"
  log_counter "Total time spent by all reduces waiting after reserving slots (ms)" "0"
  log_counter "Total time spent by all maps waiting after reserving slots (ms)" "0"
  log_counter "Launched map tasks" $paths
  log_counter "SLOTS_MILLIS_REDUCES"
  log "INFO" "mapred.JobClient" "   FileSystemCounters"
  log_counter "FILE_BYTES_READ"
  log_counter "HDFS_BYTES_READ"
  log_counter "FILE_BYTES_WRITTEN"
  log "INFO" "mapred.JobClient" "   Map-Reduce Framework"
  log_counter "Map input records"
  log_counter "Reduce shuffle bytes"
  log_counter "Spilled Records" "600"
  log_counter "Map output bytes"
  log_counter "CPU time spent (ms)" "31700"
  log_counter "Total committed heap usage (bytes)"
  log_counter "Map input bytes"
  log_counter "Combine input records"
  log_counter "SPLIT_RAW_BYTES"
  log_counter "Reduce input records"
  log_counter "Reduce input groups"
  log_counter "Combine output records"
  log_counter "Physical memory (bytes) snapshot"
  log_counter "Reduce output records"
  log_counter "Virtual memory (bytes) snapshot"
  log_counter "Map output records"
}

level=${1-"beginner"}

case "$level" in
  "beginner") 
    wait_amount=2
    ;;
  "intermediate") 
    wait_amount=10
    ;;
  "expert") 
    wait_amount=30
    ;;
  "doug") 
    wait_amount=60
    ;;
  *) echo "Unrecognised level"
    exit 1
    ;;
esac

paths=$RANDOM
let "paths %= 1000"
job_id="job_`date '+%Y%m%d%H%M_1234'`"

preamble
mapreduce
counters



