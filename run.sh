#!/bin/bash
# GHA_DEBUG=1
# GHA_SAVE_CONFIG=1
# GHA_ES_BULK_SIZE=10
# GHA_NO_INCREMENTAL=1
GHA_LOAD_CONFIG=1 GHA_DAY_FROM="`date +%F`" GHA_DAY_TO="`date +%F`" GHA_HOUR_FROM=0 GHA_HOUR_TO=now GHA_ES_URL="`cat ES_URL.test.secret`" GHA_GITHUB_OAUTH="`cat /etc/github/oauths`" ./dadsgha ~/dev/LF-Engineering/dev-analytics-api/app/services/lf/bootstrap/fixtures
