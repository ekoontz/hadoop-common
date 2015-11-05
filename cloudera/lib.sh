#!/bin/bash
set -xe

# This script ensures that the environment is properly setup for build and test of the
# corresponding component. There are two different modes of operation
# 1) Toolchain - TOOLCHAIN_HOME is defined and the environment will be derived using then
#                environment exported from it
# 2) Manual - On a developers machine, the developer is supposed to have setup the necessary
#             environment such as PATH referencing the desired JAVA, MAVEN, ANT..


# Build - produces the necessary artifacts for testing. Takes the following
# arguments
#
# - POM -- the POM to build
# - MAVEN_FLAGS -- and Maven flags, properties or options to the build
# - CODE_COVERAGE -- iff 'true' the produce code coverage artifacts
# - NO_BUILD -- iff 'true' then skip this step
function build() {
  local _POM=$1
  local _MAVEN_FLAGS=$2
  local _CODE_COVERAGE=$3
  local _NO_BUILD=$4

  if [[ "$_NO_BUILD" != "true" ]]; then
    echo
    echo ----
    echo Building: ${_POM} with "${_MAVEN_FLAGS}"
    echo ----
    echo
    if [ "$_CODE_COVERAGE" == "true" ]; then
      mvn -f ${_POM} -e install ${_MAVEN_FLAGS} -DskipTests -Pcloudera-clover
    else
      mvn -f ${_POM} -e install ${_MAVEN_FLAGS} -DskipTests
    fi
  else
    echo
    echo ----
    echo Skipping build
    echo ----
    echo
  fi
}

# Run all the tests. Takes the following arguments:
#
# - POM -- the POM to test
# - MAVEN_FLAGS -- and Maven flags, properties or options to the test
function runAllTests() {
  local _POM=$1
  local _MAVEN_FLAGS=$2

  echo
  echo ----
  echo Running all tests in ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -Pcloudera-unittest -f ${_POM} -e findbugs:findbugs checkstyle:checkstyle test ${_MAVEN_FLAGS} -Dtest.excludes.file=
}

# Run all the tests with code coverage. Will also upload the results to http://sonar.jenkins.cloudera.com:9000.
# Takes the following arguments:
#
# - POM -- the POM to test
# - MAVEN_FLAGS -- and Maven flags, properties or options to the test
# - EXCLUDES -- unstable tests that shouldn't be run
function runCodeCoverage() {
  local _POM=$1
  local _MAVEN_FLAGS=$2
  local _EXCLUDES=$3
  local _MERGED_EXCLUDES=${CLOUDERA_DIR}/merged-excludes.txt

  # merge the specified excludes with a potential code-coverage-excludes.txt file
  cat ${_EXCLUDES} ${CLOUDERA_DIR}/code-coverage-excludes.txt 2> /dev/null | sed '/^$/d' | sort > ${_MERGED_EXCLUDES}

  echo
  echo ----
  echo Running code coverage tests in ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -Pcloudera-clover -Pcloudera-unittest -f ${_POM} -e findbugs:findbugs checkstyle:checkstyle test ${_MAVEN_FLAGS} \
   clover2:aggregate clover2:clover -Dtest.excludes.file=${_MERGED_EXCLUDES}

  echo
  echo ----
  echo Publishing to sonar ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -Pcloudera-clover -f ${_POM} ${_MAVEN_FLAGS} sonar:sonar
}

# Run all the stable tests. Takes the following arguments:
#
# - POM -- the POM to test
# - MAVEN_FLAGS -- and Maven flags, properties or options to the test
# - EXCLUDES -- unstable tests that shouldn't be run
function runStableTests() {
  local _POM=$1
  local _MAVEN_FLAGS=$2
  local _EXCLUDES=$3

  echo
  echo ----
  echo Running stable tests in ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -Pcloudera-unittest -f ${_POM} -e findbugs:findbugs checkstyle:checkstyle test ${_MAVEN_FLAGS} -Dtest.excludes.file=${_EXCLUDES}
}

# Run the tests in the supplied test policy. Takes the following arguments:
#
# - POM -- the POM to test
# - MAVEN_FLAGS -- and Maven flags, properties or options to the test
# - TEST_SET -- file containing the tests to run
function runTestSet() {
  local _POM=$1
  local _MAVEN_FLAGS=$2
  local _TEST_SET=$3

  echo
  echo ----
  echo Running tests specified in ${_TEST_SET} in ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -Pcloudera-unittest -f ${_POM} -e findbugs:findbugs checkstyle:checkstyle test ${_MAVEN_FLAGS} \
    -Dtest.includes.file=${_TEST_SET} -Dtest.excludes.file=
}

# Setup the infra_tools and return the location of the infra_tools
function provisionInfraTools() {
  rm -r -f infra_tools
  wget -q http://github.mtv.cloudera.com/QE/infra_tools/archive/master.zip
  unzip -q -o master.zip
  mv infra_tools-master infra_tools
  rm master.zip

  local _INFRA_TOOLS_LOCATION=`pwd`/infra_tools
  ${_INFRA_TOOLS_LOCATION}/setupPyEnv.sh >> /dev/null

  INFRA_TOOLS_LOCATION=${_INFRA_TOOLS_LOCATION}
}

# Combines xUnit formated files into a single result file. This allows a single
# test case to be present multiple times.
function mergeTestResults() {
  if [[ -z ${INFRA_TOOLS_LOCATION} ]]; then
    echo INFRA_TOOLS_LOCATION is not defined
    exit 1
  fi

  local _INFRA_TOOLS_LOCATION=$1
  local _CLOUDERA_DIR=$2
  local _FILE_PATTERN=$3

  # merge the results into test-result.json
  ${INFRA_TOOLS_LOCATION}/xunit_merge.py --config ${_CLOUDERA_DIR}/test-info.json --test-dir `pwd` --file-pattern=${_FILE_PATTERN}
}

# Run all the flaky tests. At the end a test-result.json containing the results
# for each test invocationTakes will be rendered. the following arguments:
#
# - POM -- the POM to test
# - MAVEN_FLAGS -- and Maven flags, properties or options to the test
# - INCLUDES -- unstable tests that should be run
# - ITERATIONS -- the number of times to run the tests
function runFlakyTests() {
  local _POM=$1
  local _MAVEN_FLAGS=$2
  local _INCLUDES=$3
  local _ITERATIONS=$4

  provisionInfraTools
  for i in $(eval echo "{1..$_ITERATIONS}")
  do
    echo
    echo ----
    echo Running excluded tests in ${_POM} iteration ${i}/${_ITERATIONS} with ${_MAVEN_FLAGS}
    echo ----
    echo

    mvn -Pcloudera-unittest -f ${_POM} -e test ${_MAVEN_FLAGS} -Dtest.includes.file=${_INCLUDES} -Dtest.excludes.file=

    # merge the results, with the prior run(s)
    mergeTestResults ${INFRA_TOOLS_DIR} ${CLOUDERA_DIR} TEST.*\.xml
  done
}

# Setup Java specific variables like Java options as well as source and target specifiers. Assumes that
# JAVA7_HOME and optionally JAVA8_HOME is defined. Hence this should only be invoked when TOOLCHAIN_HOME
# is set.
#
# Takes the the following arguments
# JAVA_VERSION - the source version
#
# The outcome is that the following variables is defined
# JAVA_HOME - The home directory of Java
# JAVA_VERSION - the source Java version
# MAVEN_OPTS - Java specific maven flags
function setupJava() {
  local _JAVA_VERSION=$1

  case ${_JAVA_VERSION} in
    1.7)
      MAVEN_OPTS="-Xmx1g -Xms128m -XX:MaxPermSize=256m"
      JAVA_OPTS="-Xmx4g -Xms1g -XX:MaxPermSize=256m"
      if [[ -z $JAVA7_HOME ]]; then
        echo JAVA7_HOME is not set
        exit 1
      fi
      JAVA_HOME=${JAVA7_HOME}
      JAVA_VERSION=1.7
      ;;

    1.8)
      MAVEN_OPTS="-Xmx1g -Xms128m"
      JAVA_OPTS="-Xmx4g -Xms1g"
      if [[ -z $JAVA8_HOME ]]; then
        echo JAVA8_HOME is not set
        exit 1
      fi
      JAVA_HOME=${JAVA8_HOME}
      JAVA_VERSION=1.8
      ;;

    *)
      echo Unknown Java version ${_JAVA_VERSION}
      exit 1
      ;;
  esac

  echo -----------------------
  echo Source Java ${JAVA_VERSION} version
  echo -----------------------

  PATH=${JAVA_HOME}/bin:$PATH

  echo
  echo ---- Java version -----
  java -version
  echo -----------------------
}

function ensureDirectory() {
  local _DIR=$1
  local _MESSAGE=$2

  if [[ ! -d ${_DIR} ]]; then
    echo ${_MESSAGE}
    exit 1
  fi
}

# Ensures that the specified command is configured on the PATH.
# Takes the following arguments
#
# CMD - The command to check
# MESSAGE - The message to write if the command is missing
function ensureCommand() {
  local _CMD=$1
  local _MESSAGE=$2

  which $_CMD >> /dev/null
  local _EXTCODE=$?

  if [[ $_EXTCODE -ne 0 ]]; then
    echo $_MESSAGE
    exit $_EXTCODE
  fi
}

# Checks if a tool chain has been set. If set then the common environment will be setup.
# The tool chain is identified by the environment variable TOOLCHAIN_HOME it is expected
# to contain the necessary tools to produce the build. As a result PATH and other key
# environment variables will be setup according to the tool chain.
#
# Takes two arguments
# JAVA_VERSION - the source Java compiler
# TOOLCHAIN_HOME - (Optional) if not empty initialize using the toolchain environment
function setupToolChain() {
  local _JAVA_VERSION=$1
  local _TOOLCHAIN_HOME=$2

  if [[ ${_TOOLCHAIN_HOME} ]];  then
    echo -----------------------
    echo Using toolchain environment ${_TOOLCHAIN_HOME}
    echo -----------------------
    ensureDirectory ${_TOOLCHAIN_HOME} "TOOLCHAIN_HOME (${_TOOLCHAIN_HOME}) does not exist or is not a directory"

    if [[ -z "${PROTOC5_HOME}" ]]; then
      echo PROTOC5_HOME is not set
      exit 1
    fi
    ensureDirectory ${PROTOC5_HOME} "PROTOC5_HOME (${PROTOC5_HOME}) does not exist or is not a directory"

    if [[ -z ${ANT_HOME} ]]; then
      echo ANT_HOME is not set
      exit 1
    fi
    ensureDirectory ${ANT_HOME} "ANT_HOME (${ANT_HOME}) does not exist or is not a directory"

    if [[ -z ${MAVEN3_HOME} ]]; then
      echo MAVEN3_HOME is not set
      exit 1
    fi
    ensureDirectory ${MAVEN3_HOME} "MAVEN3_HOME (${MAVEN3_HOME}) does not exist or is not a directory"

    # append MAVEN and ANT to PATH
    PATH=${MAVEN3_HOME}/bin:${ANT_HOME}/bin:${PATH}:${PROTOC5_HOME}/bin

    setupJava ${_JAVA_VERSION}
  fi
  ensureCommand "javac" "Unable to execute javac (make sure that JAVA_HOME/PATH points to a JDK)"
  ensureCommand "mvn" "Unable to execute mvn"
  ensureCommand "protoc" "Unable to execute protoc"
  ensureCommand "ant" "Unable to execute ant"

  setupMavenFlags ${PROTOC5_HOME} ${_TOOLCHAIN_HOME}
}

# Setup the Java generated class files for specific VM version.
# The supported versions include 1.7 & 1.8. If the target version
# is successful then TARGET_JAVA_VERSION will be setup correctly.
#
# Takes the following arguments:
# TARGET-JAVA_VERSION - the target version
function setupJavaTarget() {
  local _TARGET_JAVA=$1

  case ${_TARGET_JAVA} in
    1.7|1.8)
      echo
      echo -----------------------
      echo Target Java ${_TARGET_JAVA} version
      echo -----------------------
      TARGET_JAVA_VERSION=${_TARGET_JAVA}
      ;;

    *)
      echo Unknown target Java version ${_TARGET_JAVA}
      exit 1
      ;;
  esac
}

# After the environment variables been defined used to set MAVEN_FLAGS. Takes no arguments and should be called
# during initialize() post calls to setup of toolchain
#
# Accepts the following arguments
# PROTOBUF_HOME - (Optional) home of protobuf binaries
# TOOLCHAIN_HOME - (Optional) location of Tool chain
function setupMavenFlags() {
  local _PROTOBUF_HOME=$1
  local _TOOLCHAIN_HOME=$2

  MAVEN_FLAGS="-Pnative -Drequire.fuse -Drequire.snappy -DjavaVersion=$JAVA_VERSION -DtargetJavaVersion=$TARGET_JAVA_VERSION -Dmaven.test.failure.ignore=true -Dtest.fork.count=${TEST_FORK_COUNT} -Dtest.fork.reuse=${TEST_REUSE_FORKS}"

  # setup of protobuf path, since Hadoop pom is using HADOOP_PROTOC_PATH it will be set here too, unless already
  # defined
  if [[ -z "${HADOOP_PROTOC_CDH5_PATH}" && "${_PROTOBUF_HOME}" ]]; then
    HADOOP_PROTOC_PATH=${_PROTOBUF_HOME}/bin/protoc
    MAVEN_FLAGS="${MAVEN_FLAGS} -Dcdh.protobuf.path=${_PROTOBUF_HOME}/bin/protoc"
  fi

  if [[ "${_TOOLCHAIN_HOME}" ]]; then
    MAVEN_FLAGS="${MAVEN_FLAGS} -Dcdh.toolchain.home=${_TOOLCHAIN_HOME}"
  fi
}

function printUsage() {
  echo Usage:
  echo "lib.sh --java=<1.7(default)|1.8> --target-java=<1.7(default)|1.8> --pom=<pom path> --no-build=<true|false(default)>"
  echo "       --toolchain-home=<toolchain directory> --protobuf-home=<protobuf directory> --iterations=<number>"
  echo "       --test-fork-count=<number> --test-fork-reuse=<true(default)|false> --test-set=<include-file>"
  echo
  echo "This script is intended to be invoked by one of the proxy scripts: build.sh, test-all.sh, test-code-coverage.sh, "
  echo "test-flaky.sh, test-stable.sh or test-set.sh"
  echo
  echo "Assuming this script is running under Jenkins and with toolkit env defining the following environment variables"
  echo "- ANT_HOME"
  echo "- MAVEN3_HOME"
  echo "- JAVA7_HOME"
  echo "- JAVA8_HOME (optional only needed when using Java 8)"
  echo
  echo "If WORKSPACE is not defined by environment, the current working directory is used as the WORKSPACE."
  echo "The result of parsing arguments, is that the following environment variables gets assigned:"
  echo "- POM -- the POM that will be used to drive build/testing"
  echo "- JAVA -- the Java source version"
  echo "- TARGET_JAVA -- the Java target byte code version"
  echo "- JAVA_HOME -- the home directory of the chosen Java"
  echo "- MAVEN_FLAGS -- the Maven flags, options and properties"
  echo "- JAVA_OPTS -- Java flags"
  echo "- MAVEN_OPTS -- Maven options"
  echo
  echo "Optionally the following variables could be set"
  echo "- TEST_ITERATIONS -- the number of times flaky tests should be executed"
  echo "- NO_BUILD -- iff set to true no pre-build will be performed"
  echo
  echo "About exclude and include files"
  echo
  echo "The format of the exclude/include files is defined by Maven Surefire which is a line based format."
  echo "Each line represents one or more classes to exclude or include, some special characters are allowed:"
  echo "- Lines where the first character is a '#' is considered a comment"
  echo "- Empty lines are allowed."
  echo "- '**/' is a path wildcard"
  echo "- '.*' is a file ending wildcard, otherwise .class is assumed"
  echo "- if a line contains a '#', the expression on the right of the '#' is treated as a method name"
  echo
  echo "The default exclude file for test-stable.sh and test-code-coverage.sh is 'excludes.txt'. Since some tests are"
  echo "more prone to fail during code coverage, an additional exclude file 'code-coverage-excludes.txt' is available."
  echo "This file specifies tests which that are only to be excluded during code coverage runs."
  echo
  echo "To run a custom selected set of tests, use test-set.sh and specify which tests in a include file using the "
  echo "--test-set switch."
}

# Assuming this script is running under Jenkins and with toolkit env defining the following environment variables
# - ANT_HOME
# - MAVEN3_HOME
# - JAVA7_HOME
# - JAVA8_HOME
#
#If WORKSPACE is not defined by environment, the current working directory is used as the WORKSPACE.
#The result of parsing arguments, is that the following environment variables gets assigned:
# - POM -- the POM that will be used to drive build/testing
# - JAVA_VERSION -- the Java source version
# - TARGET_JAVA -- the Java target byte code version
# - JAVA_HOME -- the home directory of the chosen Java
# - MAVEN_FLAGS -- the Maven flags, options and properties
# - JAVA_OPTS -- Java flags
# - MAVEN_OPTS -- Maven options
# - TEST_SET -- Name of a file in the cloudera folder describing which tests to run
#
# Optionally the following variables could be set
# - ITERATIONS -- the number of times flaky tests should be executed
# - NO_BUILD -- iff set to true no pre-build will be performed
function initialize() {

  # Set default values
  POM=pom.xml
  TEST_FORK_COUNT=1
  TEST_REUSE_FORKS=true
  JAVA_VERSION=1.7
  TARGET_JAVA=${JAVA_VERSION}
  # end default values

  for arg in "$@"
  do
  case ${arg} in
    --java=*)
      JAVA_VERSION="${arg#*=}"
      shift
      ;;

    --target-java=*)
      TARGET_JAVA="${arg#*=}"
      shift
      ;;

    --pom=*|-p=*)
      POM="${arg#*=}"
      shift
      ;;

    --test-iterations=*|-i=*)
      TEST_ITERATIONS="${arg#*=}"
      shift
      ;;

    --no-build=*)
      NO_BUILD="${arg#*=}"
      ;;

    --no-build)
      export NO_BUILD=true
      ;;

    --test-fork-count=*)
      TEST_FORK_COUNT="${arg#*=}"
      shift
      ;;

    --test-reuse-forks=*|--test-reuse-fork=*)
      TEST_REUSE_FORKS="${arg#*=}"
      shift
      ;;

    --protobuf-home=*|--pb-home=*)
      PROTOC5_HOME="${arg#*=}"
      shift
      ;;

   --toolchain-home=*|--tc-home=*)
      TOOLCHAIN_HOME="${arg#*=}"
      shift
      ;;

    --help|-h)
      printUsage
      exit 0
      ;;

    --script=*)
      SCRIPT="${arg#*=}"
      shift
      ;;

    --test-set=*|-tests=*)
      TEST_SET="${arg#*=}"
      shift
      ;;

    *)
      echo Unknown flag ${arg}
      ;;
  esac
  done

  if [[ "${SCRIPT}" != "test-flaky.sh" && ${TEST_ITERATIONS} ]]; then
    echo ${SCRIPT} cannot use --test-iterations, repetitive testing only works with test-flaky.sh
    exit 1
  fi

   if [[ "${SCRIPT}" != "test-set.sh" && ${TEST_SET} ]]; then
    echo ${SCRIPT} cannot use --test-set, this argument is only read by test-set.sh
    exit 1
  fi

  # WORKSPACE is normally set by Jenkins
  if [[ -z "${WORKSPACE}" ]]; then
    export WORKSPACE=`pwd`
  fi

  # always set the target java version
  setupJavaTarget ${TARGET_JAVA}
  # if toolchain is defined or specified use it to initialize the environment
  setupToolChain ${JAVA_VERSION} ${TOOLCHAIN_HOME}
  export PATH
}

################################### Main section ###################################

function main() {
  CLOUDERA_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  NAME=`basename $0`

  # script is passed to ensure arguments are compatible
  initialize $@ --script=${NAME}

  pushd `pwd` >> /dev/null
  cd ${CLOUDERA_DIR}/..

  case ${NAME} in
    build.sh)
      build pom.xml "${MAVEN_FLAGS}" ${CODE_COVERAGE} false
      ;;

    test-all.sh)
      build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
      runAllTests ${POM} "${MAVEN_FLAGS}"
      ;;

    test-code-coverage.sh)
      build pom.xml "${MAVEN_FLAGS}" true ${NO_BUILD}
      runCodeCoverage ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt"
      ;;

    test-flaky.sh)
      build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
      runFlakyTests ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt" ${TEST_ITERATIONS}
      ;;

    test-stable.sh)
      build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
      runStableTests ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt"
      ;;

    test-set.sh)
      build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
      runTestSet ${POM} "${MAVEN_FLAGS}" "${TEST_SET}"
      ;;

    *)
      echo Do not know how to handle ${NAME}
  esac

  popd >> /dev/null
}