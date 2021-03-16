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
set -e
set -o pipefail
for idx in `curl -s "${ESURL}/_cat/indices?format=json" | jq -r '.[].index' | sort | grep sds | grep github-issue`
do
  echo "starting remapping ${idx}"
  ./remap.sh "${1}" "${idx}" > "${idx}.log" 2>&1 || exit 3
  echo "done remapping ${idx}"
done
