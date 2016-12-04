#!/bin/bash

set -eu

docker build -t treasuredata_fdw_dev $(git rev-parse --show-toplevel)/docker
