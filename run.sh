#!/bin/bash
# GHA_DEBUG=1
# GHA_SAVE_CONFIG=1
# GHA_ES_BULK_SIZE=10
# GHA_NO_INCREMENTAL=1
#GHA_LOAD_CONFIG=1 GHA_DAY_FROM="`date +%F`" GHA_DAY_TO="`date +%F`" GHA_HOUR_FROM=0 GHA_HOUR_TO=now GHA_ES_URL="`cat ES_URL.test.secret`" GHA_GITHUB_OAUTH="`cat /etc/github/oauths`" ./dadsgha ~/dev/LF-Engineering/dev-analytics-api/app/services/lf/bootstrap/fixtures
#GHA_NCPUS='' GHA_NO_GHA_REPO_DATES='' GHA_NO_GHA_MAP='' GHA_DB_BULK_SIZE='' GHA_ES_BULK_SIZE='' GHA_NO_INCREMENTAL='' GHA_DEBUG=1 GHA_LOAD_CONFIG='' GHA_SAVE_CONFIG='' GHA_HOUR_FROM='' GHA_HOUR_TO='' GHA_DAY_FROM='' GHA_DAY_TO='' GHA_ES_URL="`cat ES_URL.test.secret`" GHA_DB_CONN="`cat DB_CONN.local.secret`" GHA_GITHUB_OAUTH="`cat /etc/github/oauths`" ./dadsgha ~/dev/LF-Engineering/dev-analytics-api/app/services/lf/bootstrap/fixtures 2>&1 | tee -a run.log
GHA_MEM_HEARTBEAT_GBYTES='' GHA_MAX_JSONS_GBYTES='' GHA_MAX_PARALLEL_SHAS='' GHA_NCPUS='' GHA_NO_GHA_REPO_DATES='' GHA_NO_GHA_MAP='' GHA_DB_BULK_SIZE='' GHA_ES_BULK_SIZE='' GHA_NO_INCREMENTAL='' GHA_DEBUG='' GHA_LOAD_CONFIG='' GHA_SAVE_CONFIG='' GHA_HOUR_FROM='' GHA_HOUR_TO='' GHA_DAY_FROM='' GHA_DAY_TO='' GHA_ES_URL="`cat ES_URL.test.secret`" GHA_DB_CONN="`cat DB_CONN.local.secret`" GHA_GITHUB_OAUTH="`cat /etc/github/oauths`" ./dadsgha ~/dev/LF-Engineering/dev-analytics-api/app/services/lf/bootstrap/fixtures 2>&1 | tee -a run.log
