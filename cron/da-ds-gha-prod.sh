#/bin/bash
cd /root/go/src/github.com/LF-Engineering/da-ds-gha/ || exit 1
export GHA_NCPUS=''
export GHA_NO_GHA_MAP=''
export GHA_DB_BULK_SIZE=''
export GHA_ES_BULK_SIZE=''
export GHA_NO_INCREMENTAL=''
export GHA_DEBUG=''
export GHA_LOAD_CONFIG=''
export GHA_SAVE_CONFIG=''
export GHA_HOUR_FROM=''
export GHA_HOUR_TO=''
export GHA_DAY_FROM=''
export GHA_DAY_TO=''
export GHA_NO_GHA_REPO_DATES=''
export GHA_MAX_PARALLEL_SHAS=32
export GHA_MAX_JSONS_GBYTES=20
export GHA_MEM_HEARTBEAT_GBYTES=40
export GHA_MAX_PARALLEL_API_REVIEWS=16
export GHA_ES_URL="`cat ES_URL.prod.secret`"
export GHA_DB_CONN="`cat DB_CONN.prod.secret`"
export GHA_GITHUB_OAUTH="`cat OAUTHS.secret`"
export GHA_AFFILIATION_API_URL="`cat API_URL.prod.secret`"
export AUTH0_DATA="`cat AUTH0_DATA.prod.secret`"
if [ -z "$GHA_ES_URL" ]
then
  echo "$0: missing GHA_ES_URL env variable, exiting"
  exit 1
fi
if [ -z "$GHA_DB_CONN" ]
then
  echo "$0: missing GHA_DB_CONN env variable, exiting"
  exit 2
fi
if [ -z "$GHA_GITHUB_OAUTH" ]
then
  echo "$0: missing GHA_GITHUB_OAUTH env variable, exiting"
  exit 3
fi
if [ -z "$GHA_AFFILIATION_API_URL" ]
then
  echo "$0: missing GHA_AFFILIATION_API_URL env variable, exiting"
  exit 4
fi
if [ -z "$AUTH0_DATA" ]
then
  echo "$0: missing AUTH0_DATA env variable, exiting"
  exit 5
fi
/usr/bin/da-ds-gha-cron-task.sh da-ds-gha-prod prod 1>> /tmp/da-ds-gha-prod.log 2>> /tmp/da-ds-gha-prod.err || exit $?
./cron/gh-flat.sh prod 1>> /tmp/gh-flat-prod.log 2>> /tmp/gh-flat-prod.err
