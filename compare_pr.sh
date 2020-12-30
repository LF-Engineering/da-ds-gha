#!/bin/bash
# ES_URL=...
# _ID=30090977
# curl -s "${ES_URL}/idx/_search" | jq '.hits.hits[]._source.uuid'
if [ -z "${ES_URL}" ]
then
  echo "$0: you must set ES_URL"
  exit 1
fi
if [ -z "${_ID}" ] 
then
  echo "$0: you must set _ID"
  exit 2
fi
curl -s -H 'Content-Type: application/json' "${ES_URL}/gha-cncf-grpc-github-pull_request/_search" -d "{\"query\":{\"term\":{\"id\":\"${_ID}\"}}}" | jq '.' > dadsgha.json
curl -s -H 'Content-Type: application/json' "${ES_URL}/sds-cncf-grpc-github-pull_request/_search" -d  "{\"query\":{\"term\":{\"id\":\"${_ID}\"}}}" | jq '.' > p2o.json
cat p2o.json | sort -r | uniq > tmp && mv tmp p2o.txt
cat dadsgha.json | sort -r | uniq > tmp && mv tmp dadsgha.txt
echo "da-ds:" > report.txt
echo '-------------------------------------------' >> report.txt
cat dadsgha.txt >> report.txt
echo '-------------------------------------------' >> report.txt
echo "p2o:" >> report.txt
echo '-------------------------------------------' >> report.txt
cat p2o.txt >> report.txt
echo '-------------------------------------------' >> report.txt
