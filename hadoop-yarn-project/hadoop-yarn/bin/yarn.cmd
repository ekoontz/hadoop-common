@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem The Hadoop command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME            The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   YARN_CLASSPATH       Extra Java CLASSPATH entries.
@rem
@rem   YARN_HEAPSIZE        The maximum amount of heap to use, in MB.
@rem                        Default is 1000.
@rem
@rem   YARN_{COMMAND}_HEAPSIZE overrides YARN_HEAPSIZE for a given command
@rem                           eg YARN_NODEMANAGER_HEAPSIZE sets the heap
@rem                           size for the NodeManager.  If you set the
@rem                           heap size in YARN_{COMMAND}_OPTS or YARN_OPTS
@rem                           they take precedence.
@rem
@rem   YARN_OPTS            Extra Java runtime options.
@rem
@rem   YARN_CLIENT_OPTS     when the respective command is run.
@rem   YARN_{COMMAND}_OPTS etc  YARN_NODEMANAGER_OPTS applies to NodeManager
@rem                              for e.g.  YARN_CLIENT_OPTS applies to
@rem                              more than one command (fs, dfs, fsck,
@rem                              dfsadmin etc)
@rem
@rem   YARN_CONF_DIR        Alternate conf dir. Default is ${HADOOP_YARN_HOME}/conf.
@rem
@rem   YARN_ROOT_LOGGER     The root appender. Default is INFO,console
@rem

setlocal enabledelayedexpansion

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

set DEFAULT_LIBEXEC_DIR=%HADOOP_BIN_PATH%\..\libexec
if not defined HADOOP_LIBEXEC_DIR (
  set HADOOP_LIBEXEC_DIR=%DEFAULT_LIBEXEC_DIR%
)

call %DEFAULT_LIBEXEC_DIR%\yarn-config.cmd %*
if "%1" == "--config" (
  shift
  shift
)

:main
  if exist %YARN_CONF_DIR%\yarn-env.cmd (
    call %YARN_CONF_DIR%\yarn-env.cmd
  )

  set yarn-command=%1
  call :make_command_arguments %*

  if not defined yarn-command (
      goto print_usage
  )

  @rem JAVA and JAVA_HEAP_MAX and set in hadoop-config.cmd

  if defined YARN_HEAPSIZE (
    @rem echo run with Java heapsize %YARN_HEAPSIZE%
    set JAVA_HEAP_MAX=-Xmx%YARN_HEAPSIZE%m
  )

  @rem CLASSPATH initially contains HADOOP_CONF_DIR & YARN_CONF_DIR
  if not defined HADOOP_CONF_DIR (
    echo No HADOOP_CONF_DIR set. 
    echo Please specify it either in yarn-env.cmd or in the environment.
    goto :eof
  )

  set CLASSPATH=%HADOOP_CONF_DIR%;%YARN_CONF_DIR%;%CLASSPATH%

  @rem for developers, add Hadoop classes to CLASSPATH
  if exist %HADOOP_YARN_HOME%\yarn-api\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-api\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-mapreduce\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-mapreduce\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-master-worker\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-master-worker\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\build\test\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\test\classes
  )

  if exist %HADOOP_YARN_HOME%\build\tools (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\tools
  )

  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\*
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_LIB_JARS_DIR%\*

  call :%yarn-command% %yarn-command-arguments%

  if defined JAVA_LIBRARY_PATH (
    set YARN_OPTS=%YARN_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
  )

  set java_arguments=%JAVA_HEAP_MAX% %YARN_OPTS% -classpath %CLASSPATH% %CLASS% %yarn-command-arguments%
  call %JAVA% %java_arguments%

goto :eof

:classpath
  @echo %CLASSPATH%
  goto :eof

:rmadmin
  set CLASS=org.apache.hadoop.yarn.server.resourcemanager.tools.RMAdmin
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:application
  set CLASS=org.apache.hadoop.yarn.client.cli.ApplicationCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:node
  set CLASS=org.apache.hadoop.yarn.client.cli.NodeCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:resourcemanager
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\rm-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
  set YARN_OPTS=%YARN_OPTS% %HADOOP_RESOURCEMANAGER_OPTS%
  if defined YARN_RESOURCEMANAGER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_RESOURCEMANAGER_HEAPSIZE%m
  )
  goto :eof

:nodemanager
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\nm-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.nodemanager.NodeManager
  set YARN_OPTS=%YARN_OPTS% -server %HADOOP_NODEMANAGER_OPTS%
  if defined YARN_NODEMANAGER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_NODEMANAGER_HEAPSIZE%m
  )
  goto :eof

:proxyserver
  set CLASS=org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer
  set YARN_OPTS=%YARN_OPTS% %HADOOP_PROXYSERVER_OPTS%
  if defined YARN_PROXYSERVER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_PROXYSERVER_HEAPSIZE%m
  )
  goto :eof

:version
  set CLASS=org.apache.hadoop.util.VersionInfo
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:jar
  set CLASS=org.apache.hadoop.util.RunJar
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:logs
  set CLASS=org.apache.hadoop.yarn.logaggregation.LogDumper
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:daemonlog
  set CLASS=org.apache.hadoop.log.LogLevel
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%1" == "--config" (
    shift
    shift
  )
  if [%2] == [] goto :eof
  shift
  set _yarnarguments=
  :MakeCmdArgsLoop 
  if [%1]==[] goto :EndLoop 

  if not defined _yarnarguments (
    set _yarnarguments=%1
  ) else (
    set _yarnarguments=!_yarnarguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 
  set yarn-command-arguments=%_yarnarguments%
  goto :eof

:print_usage
  @echo Usage: yarn [--config confdir] COMMAND
  @echo        where COMMAND is one of:
  @echo   resourcemanager      run the ResourceManager
  @echo   nodemanager          run a nodemanager on each slave
  @echo   historyserver        run job history servers as a standalone daemon
  @echo   rmadmin              admin tools
  @echo   version              print the version
  @echo   jar ^<jar^>          run a jar file
  @echo   application          prints application(s) report/kill application
  @echo   node                 prints node report(s)
  @echo   logs                 dump container logs
  @echo   classpath            prints the class path needed to get the
  @echo                        Hadoop jar and the required libraries
  @echo   daemonlog            get/set the log level for each daemon
  @echo   or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo Most commands print help when invoked w/o parameters.

endlocal
