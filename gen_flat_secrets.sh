#!/bin/bash
# flat secret should have all spaces replaced with :space: `%s/ /:space:/g`
# env variable values should have = replaced with :eq: (but not = in `export ENVNAME=ENVVALUE`)
if [ -z "$1" ]
then
  echo "$0: you need to specify env: tst|prod as a 1st argument"
  exit 1
fi
if [ ! -f "flat.$1.secret" ]
then
  echo "$0: mssing 'flat.$1.secret' file"
  exit 2
fi
for f in `cat "flat.$1.secret"`
do
  ary=(${f//=/ })
  name=${ary[0]//export:space:/}
  val=${ary[1]//:eq:/=}
  val=${val//:space:/ }
  val=${val//\"/}
  val=${val//\'/}
  echo "$name --> $val"
  echo -n "$val" > "flat_secrets/${name}.$1.secret"
done
