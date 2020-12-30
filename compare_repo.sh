#!/bin/bash
# ES_URL=...
# _ID1=d80ed376996a4376beec7857ce5436c04789e6c0
# _ID2=20442ec6fb76fed9cda8d597e122209881fde249
# curl -s "${ES_URL}/idx/_search" | jq '.hits.hits[]._source.uuid'
if [ -z "${ES_URL}" ]
then
  echo "$0: you must set ES_URL"
  exit 1
fi
if [ -z "${_ID1}" ] 
then
  echo "$0: you must set _ID1"
  exit 2
fi
if [ -z "${_ID2}" ] 
then
  echo "$0: you must set _ID2"
  exit 2
fi
curl -s -H 'Content-Type: application/json' "${ES_URL}/gha-cncf-grpc-github-repository/_search" -d "{\"query\":{\"term\":{\"_id\":\"${_ID1}\"}}}" | jq '.' > dadsgha.json
curl -s -H 'Content-Type: application/json' "${ES_URL}/sds-cncf-grpc-github-repository/_search" -d  "{\"query\":{\"term\":{\"_id\":\"${_ID2}\"}}}" | jq '.' > p2o.json
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
