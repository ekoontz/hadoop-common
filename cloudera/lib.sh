#!/bin/bash

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

  echo
  echo ----
  echo Running code coverage tests in ${_POM} with ${_MAVEN_FLAGS}
  echo ----
  echo
  mvn -f -Pcloudera-clover -Pcloudera-unittest ${_POM} -e findbugs:findbugs checkstyle:checkstyle test ${_MAVEN_FLAGS} -Dtest.excludes.file=${_EXCLUDES}

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

# Setup the infra_tools and return the location of the infra_tools
function provisionInfraTools() {
  rm -r -f infra_tools
  wget -q http://github.mtv.cloudera.com/QE/infra_tools/archive/master.zip
  unzip -q -o master.zip
  mv infra_tools-master infra_tools
  rm master.zip

  local _INFRA_TOOLS_LOCATION=`pwd`/infra_tools
  ${_INFRA_TOOLS_LOCATION}/setupPyEnv.sh

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

  # merge the results
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
# JAVA_7_HOME and optionally JAVA_8_HOME is defined.
#
# Takes the the following arguments
#
# JAVA - the source version
# TARGET-JAVA - the target version
#
# The outcome is that the following variables is defined
# JAVA_HOME - The home directory of Java
# JAVA_VERSION - the source Java version
# JAVA_TARGET_VERSION - The targeted Java version
# MAVEN_OPTS - Java specific maven options
function setupJava() {
  local _JAVA=$1
  local _TARGET_JAVA=$2

  case ${_JAVA} in
    1.7)
      MAVEN_OPTS="-Xmx1g -Xms128m -XX:MaxPermSize=256m"
      JAVA_OPTS="-Xmx4g -Xms1g -XX:MaxPermSize=256m"
      if [[ -z $JAVA_7_HOME ]]; then
        echo JAVA_7_HOME is not set
        exit 1
      fi
      JAVA_HOME=${JAVA_7_HOME}
      JAVA_VERSION=1.7
      ;;

    1.8)
      MAVEN_OPTS="-Xmx1g -Xms128m"
      JAVA_OPTS="-Xmx4g -Xms1g"
      if [[ -z $JAVA_8_HOME ]]; then
        echo JAVA_8_HOME is not set
        exit 1
      fi
      JAVA_HOME=${JAVA_8_HOME}
      JAVA_VERSION=1.8
      ;;

    *)
      echo Unknown Java version ${_JAVA}
      exit 1
      ;;
  esac

  echo ----
  echo Source Java ${JAVA_VERSION} version
  echo ----

  case ${_TARGET_JAVA} in
    1.7|1.8)
      echo
      echo ----
      echo Target Java ${TARGET_JAVA} version
      echo ----
      TARGET_JAVA_VERSION=${_TARGET_JAVA}
      ;;

    *)
      echo Unknown target Java version ${_TARGET_JAVA}
      exit 1
      ;;
  esac

  echo
  echo ---- Java version -----
  ${JAVA_HOME}/bin/java -version
  echo ----
}

function printUsage() {
  echo Usage:
  echo "lib.sh --java=<1.7(default)|1.8> --target-java=<1.7(default)|1.8> --pom=<pom path> --no-build=<true|false(default)>"
  echo "       --toolchain-home=<toolchain directory> --protobuf-home=<protobuf directory> --iterations=<number>"
  echo "       --test-fork-count=<number> --test-fork-reuse=<true(default)|false>"
  echo
  echo "This script is intended to be invoked by one of the proxy links: build, test-all, test-code-coverage, test-flaky"
  echo "and test-stable"
  echo
  echo "Assume that this script is running using Jenkins and with toolkit defining the following environment variables"
  echo "- ANT_1_8_1_HOME"
  echo "- MAVEN_3_0_HOME"
  echo "- JAVA_7_HOME"
  echo "- JAVA_8_HOME (optional only needed when using Java 8)"
  echo
  echo "If WORKSPACE is not defined by environment, the current working directory is assumed as the WORKSPACE.
  echo "The result of parsing arguments is that the following envionment variables will be assigned
  echo "- POM -- the POM that will be used to drive build/testing
  echo "- JAVA -- the Java source version
  echo "- TARGET_JAVA -- the Java target byte code version"
  echo "- JAVA_HOME -- the home directory of the choosen Java"
  echo "- MAVEN_FLAGS -- the Maven flags, options and properties"
  echo "- JAVA_OPTS -- Java flags"
  echo "- MAVEN_OPTS -- Maven options"
  echo
  echo "Optionally the following variables could be set"
  echo "- TEST_ITERATIONS -- the number of times flaky tests should be executed"
  echo "- NO_BUILD -- iff set to true no pre-build will be performed"
}

# Assume that this script is running using Jenkins and with toolkit defining the following environment variables
# - ANT_1_8_1_HOME
# - MAVEN_3_0_HOME
# - JAVA_7_HOME
# - JAVA_8_HOME
#
# If WORKSPACE is not defined by environment, the current working directory is assumed as the WORKSPACE.
# The result of parsing arguments is that the following envionment variables will be assigned
# - POM -- the POM that will be used to drive build/testing
# - JAVA -- the Java source version
# - TARGET_JAVA -- the Java target byte code version
# - JAVA_HOME -- the home directory of the choosen Java
# - MAVEN_FLAGS -- the Maven flags, options and properties
# - JAVA_OPTS -- Java flags
# - MAVEN_OPTS -- Maven options
#
# Optionally the following variables could be set
# - ITERATIONS -- the number of times flaky tests should be executed
# - NO_BUILD -- iff set to true no pre-build will be performed
function initialize() {

  # Set default values
  POM=pom.xml
  TEST_FORK_COUNT=1
  TEST_REUSE_FORKS=true
  JAVA=1.7
  TARGET_JAVA=${JAVA}
  # end default values

  if [[ -z $ANT_1_8_1_HOME ]]; then
    echo ANT_1_8_1_HOME is not set
    exit 1
  fi
  ANT_HOME=$ANT_1_8_1_HOME

  if [[ -z $MAVEN_3_0_HOME ]]; then
    echo MAVEN_3_0_HOME is not set
    exit 1
  fi
  MAVEN_HOME=$MAVEN_3_0_HOME

  for arg in "$@"
  do
  case ${arg} in
    --java=*)
      JAVA="${arg#*=}"
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
      PROTOBUF_HOME="${arg#*=}"
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

    --function=*)
      FUNCTION="${arg#*=}"
      shift
      ;;

    *)
      echo Unknown flag ${arg}
      ;;
  esac
  done

  if [[ "${FUNCTION}" != "test-flaky" && ${TEST_ITERATIONS} ]]; then
    echo ${FUNCTION} cannot use --test-iterations, repetitive testing only works with test-flaky
    exit 1
  fi

  setupJava ${JAVA} ${TARGET_JAVA}
  export PATH=${JAVA_HOME}/bin:${MAVEN_HOME}/bin:${ANT_HOME}/bin:${PATH}

  if [[ -z "$WORKSPACE" ]]; then
    export WORKSPACE=`pwd`
  fi

  MAVEN_FLAGS="-Pnative -Drequire.fuse -Drequire.snappy -Dmaven.repo.local=$WORKSPACE/.m2 -DjavaVersion=$JAVA_VERSION -DtargetJavaVersion=$TARGET_JAVA_VERSION -Dmaven.test.failure.ignore=true -Dtest.fork.count=${TEST_FORK_COUNT} -Dtest.fork.reuse=${TEST_REUSE_FORKS}"

  if [[ "${PROTOBUF_HOME}" ]]; then
    MAVEN_FLAGS="${MAVEN_FLAGS} -Dcdh.protobuf.home=${PROTOBUF_HOME}"
  fi

  if [[ "${TOOLCHAIN_HOME}" ]]; then
    MAVEN_FLAGS="${MAVEN_FLAGS} -Dcdh.toolchain.home=${TOOLCHAIN_HOME}"
  fi

}

################################### Main section ###################################

CLOUDERA_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=`basename $0`
initialize $@ --function=${NAME}

pushd `pwd` >> /dev/null
cd ${CLOUDERA_DIR}/..

case ${NAME} in
  build)
    build pom.xml "${MAVEN_FLAGS}" ${CODE_COVERAGE} false
    ;;

  test-all)
    build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
    runAllTests ${POM} "${MAVEN_FLAGS}"
    ;;

  test-code-coverage)
    build pom.xml "${MAVEN_FLAGS}" true ${NO_BUILD}
    runCodeCoverage ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt"
    ;;

  test-flaky)
    build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
    runFlakyTests ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt" ${TEST_ITERATIONS}
    ;;

  test-stable)
    build pom.xml "${MAVEN_FLAGS}" false ${NO_BUILD}
    runStableTests ${POM} "${MAVEN_FLAGS}" "${CLOUDERA_DIR}/excludes.txt"
    ;;

  *)
    echo Do not know how to handle ${NAME}
esac

popd >> /dev/null
