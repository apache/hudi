#!/bin/bash

set -x

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH

export HUDI_QUIETER_LOGGING=1

# Clean and build
mvn clean compile test-compile install -DskipTests -DskipITs

# Run unit tests in parallel
mvn test -Punit-tests -pl !hudi-client -B >log1.txt 2>&1 &
PIDS[0]=$!
mvn test -Punit-tests -pl hudi-client -B >log2.txt 2>&1 &
PIDS[1]=$!
mvn test -Pfunctional-tests -B >log3.txt 2>&1 &
PIDS[2]=$!

# Wait for completion
ERR=0
for pid in ${PIDS[*]}; do
  wait $pid
  if [ $? -ne 0 ]; then
    echo "FAILED"
    ERR=1 
  fi
done

# Test output
cat log1.txt  log2.txt log3.txt

# Convert jacoco coverage report to cobertura format (recognized by https://github.com/uber/phabricator-jenkins-plugin)
.jenkins/jacoco2cobertura.sh

exit $ERR

