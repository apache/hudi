#!/bin/bash

set -xe

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH

export HUDI_QUIETER_LOGGING=1

# Run unit tests
mvn clean compile test-compile install -DskipTests -DskipITs
mvn test -Punit-tests -B
mvn test -Pfunctional-tests -B

# Convert jacoco coverage report to cobertura format (recognized by https://github.com/uber/phabricator-jenkins-plugin)
.jenkins/jacoco2cobertura.sh

