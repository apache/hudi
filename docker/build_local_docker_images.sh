#!/bin/bash

while true; do
    read -p  "Docker images can be downloaded from docker hub and seamlessly mounted with latest HUDI jars. Do you still want to build docker images from scratch ?" yn
    case $yn in
        [Yy]* ) make install; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done
pushd ../
mvn clean pre-integration-test -DskipTests -Ddocker.compose.skip=true -Ddocker.build.skip=false
popd
