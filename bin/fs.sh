#!/bin/sh

export CLASSPATH="/Users/ekoontz/hadoop-hdfs/build/classes:\
/Users/ekoontz/hadoop-common/build/classes:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-cli-1.2.jar:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-lang-2.5.jar:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-logging-1.1.1.jar"

java org.apache.hadoop.fs.FsShell $*

