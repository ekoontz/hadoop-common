#!/bin/bash
set -xe

DIR="$( cd $( dirname $( dirname ${BASH_SOURCE[0]} ) ) && pwd )"
cd $DIR

$DIR/cloudera/build.sh