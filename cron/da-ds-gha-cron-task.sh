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
lock_file="/tmp/$1.lock"
if [ -f "${lock_file}" ]
then
  echo "$0: another da-ds-gha \"$2\" instance \"$1\" is still running, exiting"
  exit 3
fi
cd /root/go/src/github.com/LF-Engineering/da-ds-gha/ || exit 4
git pull || exit 5
repo="`cat repo_access.secret`"
if [ -z "$repo" ]
then
  echo "$0: missing repo_access.secret file"
  exit 6
fi
rm -rf dev-analytics-api
git clone "${repo}" || exit 7
cd dev-analytics-api || exit 8
git checkout "$2" || exit 9
cd .. || exit 10
function cleanup {
  rm -rf "${lock_file}" dev-analytics-api
}
> "${lock_file}"
trap cleanup EXIT
#./dadsgha ./dev-analytics-api/app/services/lf/bootstrap/fixtures 2>&1 | tee -a run.log
./dadsgha ./dev-analytics-api/app/services/lf/bootstrap/fixtures
