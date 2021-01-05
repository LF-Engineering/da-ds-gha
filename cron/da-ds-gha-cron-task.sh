#!/bin/bash
date
if [ -z "$1" ]
then
  echo "$0: you need to specify name for this run, can be 'da-ds-gha-test' for example"
  exit 1
fi
if [ -z "$2" ]
then
  echo "$0: you need to specify environment as a 2nd arg: test|prod"
  exit 2
fi
cd /root/go/src/github.com/LF-Engineering/da-ds-gha/ || exit 3
git pull || exit 4
repo="`cat repo_access.secret`"
if [ -z "$repo" ]
then
  echo "$0: missing repo_access.secret file"
  exit 5
fi
rm -rf dev-analytics-api
git clone "${repo}" || exit 6
cd dev-analytics-api || exit 7
git checkout "$2" || exit 8
cd .. || exit 9
lock_file="/tmp/$1.lock"
function cleanup {
  rm -f "${lock_file}" dev-analytics-api
}
if [ -f "${lock_file}" ]
then
  # echo "$0: another da-ds-gha instance \"$1\" is still running, exiting"
  exit 10
fi
> "${lock_file}"
trap cleanup EXIT
./dadsgha ./dev-analytics-api/app/services/lf/bootstrap/fixtures 2>&1 | tee -a run.log
