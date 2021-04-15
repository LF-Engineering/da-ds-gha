#!/bin/bash
repo="`cat ./repo_access.secret`"
if [ -z "$repo" ]
then
  echo "$0: missing ./repo_access.secret file"
  exit 1
fi
metrics_repo="`cat ./metrics_repo_access.secret`"
if [ -z "$metrics_repo" ]
then
  echo "$0: missing ./metrics_repo_access.secret file"
  exit 2
fi
if [ -z "$1" ]
then
  echo "$0: you need to specify env as a 1st arg: test|prod"
  exit 3
fi
for e in ELASTIC_URL ELASTIC_USERNAME ELASTIC_PASSWORD API_DB_ENDPOINT SH_DB_ENDPOINT \
         AUTH0_GRANT_TYPE AUTH0_CLIENT_ID AUTH0_CLIENT_SECRET AUTH0_AUDIENCE \
         GITHUB_OAUTH_API_TOKEN ELASTIC_CACHE_URL ELASTIC_CACHE_USERNAME ELASTIC_CACHE_PASSWORD \
         STAGE AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
         CRON_RUN_FOR GAP_HANDLER_URL
do
  fn="flat_secrets/${e}.${1}.secret"
  value="`cat ${fn}`"
  if [ -z "$value" ]
  then
    echo "$0: you need to provide ${fn} file"
    exit 4
  fi
  export $e="${value}"
done
cd .. || exit 5
if [ -d "dev-analytics-metrics" ]
then
  cd dev-analytics-metrics || exit 6
  git reset --hard || exit 17
  git pull || exit 7
else
  git clone "${metrics_repo}" || exit 8
  cd dev-analytics-metrics || exit 9
fi
make swagger || exit 10
CGO_ENABLED=0 go build -ldflags '-s -w' -o ./gh-flat-binary ./ghprflat/github_pr_flat_structure_handler.go || exit 11
# Now get all non-disabled fixtures from dev-analytics-api for a given env branch
function cleanup {
  rm -rf dev-analytics-api
}
git clone "${repo}" || exit 12
trap cleanup EXIT
cd dev-analytics-api || exit 13
git checkout "$1" || exit 14
cd .. || exit 15
cmdline='./gh-flat-binary '
if [ ! -z "$YQ3" ]
then
  for f in `find dev-analytics-api/app/services/lf/bootstrap/fixtures/ -type f -iname "*.y*ml"`
  do
    disabled=`yq r "$f" 'disabled'`
    if [ -z "$disabled" ]
    then
      slug=`yq r "$f" 'native.slug'`
      cmdline="$cmdline \"$slug\""
    fi
  done
else
  for f in `find dev-analytics-api/app/services/lf/bootstrap/fixtures/ -type f -iname "*.y*ml"`
  do
    disabled=`yq e '.disabled' "$f"`
    if [ ! "$disabled" = "true" ]
    then
      slug=`yq e '.native.slug' "$f"`
      cmdline="$cmdline \"$slug\""
    fi
  done
fi
echo $cmdline
eval $cmdline || exit 16
echo 'OK'
