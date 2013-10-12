#!/bin/zsh

KRONOS_URL=http://localhost:8150

KEYS=(apple banana coconut donut fig pear orange kvass grapes tomato strawberry watermelon)
N=${1:-1}

function add_point() {
  data=()
  for key in $KEYS; do
    data+=( "\"$key\":$(($RANDOM%10))" )
  done
  data=${(j:,:)data}
  data="{\"fruits\":[{${data}}]}"
  echo "${data}" | lwp-request -m POST "${KRONOS_URL}/1.0/events/put"
  sleep 1
}

repeat $N add_point

# Retrieve stored events
#echo '{"stream":"fruits","start_time":0,"end_time":24774537120000000}' | lwp-request -m POST ${KRONOS_URL}/1.0/events/get

