#!/bin/sh

export CLASSPATH="/Users/ekoontz/hadoop-hdfs/build/classes:\
/Users/ekoontz/hadoop-common/build/classes:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-cli-1.2.jar:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-lang-2.5.jar:\
/Users/ekoontz/hadoop-common/build/ivy/lib/Hadoop-Common/common/commons-logging-1.1.1.jar"

#export DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=n"

export FLAGS="-Xmx1000m \
-Dhadoop.log.dir=/Users/ekoontz/hadoop-common/bin/../logs \
-Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/Users/ekoontz/hadoop-common/bin/.. \
-Dhadoop.id.str= -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml"

java $FLAGS org.apache.hadoop.fs.FsShell $*

