#!/bin/bash
for env in test prod
do
  echo "env: $env"
  es_url=`cat "ES_URL.${env}.secret"`
  for idx in `curl -s "${es_url}/_cat/indices" | sort | grep '\-github\-' | grep 'gha\-' | awk '{print $3}'`
  do
    echo "$env: $idx"
    curl -XDELETE "${es_url}/${idx}"
    echo ''
  done
done
