#!/bin/zsh

KRONOS_URL=http://localhost:8150

STREAM=$1
START=$2
END=$3

lwp-request -m POST ${KRONOS_URL}/1.0/events/get <<< "{ \"stream\": \"$1\", \"start_time\": $2, \"end_time\": $3 }"

# Retrieve stored events
#echo '{"stream":"fruits","start_time":0,"end_time":24774537120000000}' | lwp-request -m POST ${KRONOS_URL}/1.0/events/get

