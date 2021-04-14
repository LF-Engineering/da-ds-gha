#!/bin/bash
slugs=''
for f in `find dev-analytics-api/app/services/lf/bootstrap/fixtures/ -type f -iname "*.y*ml"`
do
  disabled=`yq r "$f" 'disabled'`
  if [ -z "$disabled" ]
  then
    slug=`yq r "$f" 'native.slug'`
    if [ -z "$slugs" ]
    then
      slugs=$slug
    else
      slugs="$slugs $slug"
    fi
    echo $slugs
  else
    echo "$f is disabled"
  fi
done
