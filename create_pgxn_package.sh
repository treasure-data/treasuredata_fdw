#!/usr/bin/env bash

version=$(cat META.json | jq -c -r .version)

git archive --format zip --prefix=treasuredata_fdw-${version}/ --output treasuredata_fdw-${version}.zip master
