#!/bin/bash
repo="`cat ./flat_secrets/repo_access.secret`"
if [ -z "$repo" ]
then
  echo "$0: missing ./flat_secrets/repo_access.secret file"
  exit 1
fi
if [ -z "$1" ]
then
  echo "$0: you need to specify env as a 1st arg: test|prod"
  exit 2
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
    exit 3
  fi
  export $e="${value}"
done
cd ..
if [ -d "dev-analytics-metrics" ]
then
  cd dev-analytics-metrics || exit 4
  git pull || exit 5
else
  git clone https://github.com/LF-Engineering/dev-analytics-metrics.git || exit 6
  cd dev-analytics-metrics || exit 7
fi
make swagger || exit 8
CGO_ENABLED=0 go build -ldflags '-s -w' -o ./gh-flat-binary ./ghprflat/github_pr_flat_structure_handler.go || exit 9
# Now get all non-disabled fixtures from dev-analytics-api for a given env branch
function cleanup {
  rm -rf dev-analytics-api
}
git clone "${repo}" || exit 10
trap cleanup EXIT
cd dev-analytics-api || exit 11
git checkout "$1" || exit 12
cd .. || exit 13
cmdline='./gh-flat-binary '
for f in `find dev-analytics-api/app/services/lf/bootstrap/fixtures/ -type f -iname "*.y*ml"`
do
  disabled=`yq r "$f" 'disabled'`
  if [ -z "$disabled" ]
  then
    slug=`yq r "$f" 'native.slug'`
    cmdline="$cmdline \"$slug\""
  fi
done
echo $cmdline
env | grep API_DB_ENDPOINT
eval $cmdline
