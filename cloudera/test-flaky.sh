#!/bin/bash
set -xe

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. $DIR/lib.sh
main $@
