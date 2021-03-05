#!/bin/bash
oauth="`cat /etc/github/oauth`"
curl -XGET -u "${oauth}:x-oauth-basic" 'https://api.github.com/repos/kubernetes/kubernetes/pulls/6718/reviews'
