#!/bin/bash
declare -A glog
declare -A gfile
for f in `find .git/objects/??/ -type f | sed 's/\(.*\)\/\([[:xdigit:]]\{2\}\)\/\([[:xdigit:]]\+\)$/\2\3/g'`
do
  gfile[$f]=1
done
for f in `git rev-list HEAD`
do
  glog[$f]=1
done
for f in "${!gfile[@]}"
do
  got=${glog[$f]}
  if [ ! "$got" = "1" ]
  then
    echo "missing file SHA $f"
  fi
done
for f in "${!glog[@]}"
do
  got=${gfile[$f]}
  if [ ! "$got" = "1" ]
  then
    echo "missing log SHA $f"
  fi
done
