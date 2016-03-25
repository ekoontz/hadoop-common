#!/bin/bash
set -xe

DIR="$( cd $( dirname ${BASH_SOURCE[0]} )  && pwd )"
cd $DIR

# Build the project
$DIR/build.sh

# Install supertest locally
SCRIPTS="supertest"

if [[ -d $SCRIPTS ]]; then
    echo "Cleaning up remnants from a previous run"
    rm -rf $SCRIPTS
fi

git clone --depth 1 git://github.mtv.cloudera.com/CDH/$SCRIPTS.git $SCRIPTS || true

# Fetch the right branch
cd "$DIR/$SCRIPTS"
git fetch --depth 1 origin
git checkout -f origin/master
git ls-tree -r HEAD
./init.sh
git submodule status
./make-config.sh
# Activate the supertest virtualenv
source ./supertest-env/bin/activate
export PATH=`pwd`/grind/bin/:$PATH
which grind

# Go to project root
cd "$DIR/.."

cat > .grind_deps << EOF
{
        "empty_dirs": ["test/data", "test-dir", "log"],
        "file_patterns": ["*.so"]
}
EOF

# Invoke grind to run tests
grind -c ${DIR}/supertest/grind.cfg config
grind -c ${DIR}/supertest/grind.cfg test --artifacts -r 3 -e TestRM -e TestWorkPreservingRMRestart -e TestRMRestart -e TestContainerAllocation -e TestMRJobClient -e TestCapacityScheduler -e TestDelegatingInputFormat -e TestMRCJCFileInputFormat -e TestJobHistoryEventHandler -e TestCombineFileInputFormat -e TestAMRMRPCResponseId -e TestSystemMetricsPublisher -e TestNodesListManager -e TestRMContainerImpl -e TestApplicationMasterLauncher -e TestRMWebApp -e TestContainerManagerSecurity -e TestResourceManager -e TestParameterParser -e TestNativeCodeLoader -e TestRMContainerAllocator -e TestMRIntermediateDataEncryption -e TestWebApp -e TestCryptoStreamsWithOpensslAesCtrCryptoCodec -e TestDNS
# TestDNS fails only on supertest. CDH-37451

# Cleanup the grind folder
if [[ -d "$DIR/$SCRIPTS" ]]; then
    rm -rf "$DIR/$SCRIPTS"
fi
