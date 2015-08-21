#!/bin/bash
set -xe

SCRIPTS="jenkins-job-scripts"
DIR="$( cd $( dirname $( dirname ${BASH_SOURCE[0]} ) ) && pwd )"
cd $DIR

if [[ -d $SCRIPTS ]]; then
    echo "Cleaning up remnants from a previous run"
    rm -rf $SCRIPTS
fi

# Clone the jenkins script repo
git clone --depth 1 git://github.mtv.cloudera.com/CDH/$SCRIPTS.git $SCRIPTS || true

# Fetch the right branch
cd $SCRIPTS
git fetch --depth 1 origin
git checkout -f origin/master
git ls-tree -r HEAD
cd ..

# Run the build and tests
./jenkins-job-scripts/run_precommit.sh

if [[ -d $SCRIPTS ]]; then
    rm -rf $SCRIPTS
fi
