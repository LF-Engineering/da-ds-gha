#!/bin/bash
if [ -z "${1}" ]
then
  echo "$0: you must provide environment as a 1st argument:  prod|test"
  exit 1
fi
fn="ES_URL.${1}.secret"
ESURL="`cat ${fn}`"
if [ -z "${ESURL}" ]
then
  echo "$0: missing $fn file"
  exit 2
fi
if [ -z "${2}" ]
then
  echo "$0: you need to provide an index name as a 2nd argument: index-name"
  exit 3
fi
set -o pipefail
fromidx="${2}"
toidx="${2}-`cat /dev/urandom | tr -dc 'a-z' | fold -w 32 | head -n 1`"
maybe_wait () {
  rc="${1}"
  idx="${2}"
  if [ ! "${rc}" = "0" ]
  then
    run=`curl -s "${ESURL}/_tasks?actions=*reindex&detailed" | grep "${idx}"`
    if [ -z "${run}" ]
    then
      echo "exit code is '$rc' and running is '$run'"
      return 1
    fi
    while true
    do
      echo -n '.'
      sleep 5
      run=`curl -s "${ESURL}/_tasks?actions=*reindex&detailed" | grep "${idx}"`
      if [ -z "${run}" ]
      then
        break
      fi
    done
    echo "finished reindexing to ${idx}"
  fi
  return 0
}
echo curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"source\":{\"index\":\"${fromidx}\"},\"dest\":{\"index\":\"${toidx}\"}}"
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"source\":{\"index\":\"${fromidx}\"},\"dest\":{\"index\":\"${toidx}\"}}" | jq '.'
maybe_wait $? "${toidx}"
if [ ! $? = "0" ]
then
  echo "reindexing ${fromidx} -> ${toidx} failed"
  exit 4
fi
echo curl -s -XDELETE "${ESURL}/${fromidx}"
curl -s -XDELETE "${ESURL}/${fromidx}" | jq '.' || exit 4
echo curl -s -XPUT "${ESURL}/${fromidx}"
curl -s -XPUT "${ESURL}/${fromidx}" | jq '.' || exit 5
echo curl -s -XPUT -H 'Content-Type: application/json' "${ESURL}/${fromidx}/_mapping" -d'{"properties":{"all_assignees_data":{"type":"nested"},"all_requested_reviewers_data":{"type":"nested"},"assignees_data":{"type":"nested"},"commenters_data":{"type":"nested"},"requested_reviewers_data":{"type":"nested"},"reviewer_data":{"type":"nested"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}'
curl -s -XPUT -H 'Content-Type: application/json' "${ESURL}/${fromidx}/_mapping" -d'{"properties":{"all_assignees_data":{"type":"nested"},"all_requested_reviewers_data":{"type":"nested"},"assignees_data":{"type":"nested"},"commenters_data":{"type":"nested"},"requested_reviewers_data":{"type":"nested"},"reviewer_data":{"type":"nested"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}' | jq '.' || exit 6
echo curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"source\":{\"index\":\"${toidx}\"},\"dest\":{\"index\":\"${fromidx}\"}}"
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"source\":{\"index\":\"${toidx}\"},\"dest\":{\"index\":\"${fromidx}\"}}" | jq '.'
maybe_wait $? "${toidx}"
if [ ! $? = "0" ]
then
  echo "reindexing ${toidx} -> ${fromidx} failed"
  exit 7
fi
echo curl -s -XDELETE "${ESURL}/${toidx}"
curl -s -XDELETE "${ESURL}/${toidx}" | jq '.' || exit 8
echo "${fromidx} remapped"
