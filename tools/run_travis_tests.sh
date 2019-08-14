#!/bin/bash

mode=$1

if [ "$mode" = "unit" ];
then
  echo "Running Unit Tests"
  mvn test -DskipITs=true -B
elif [ "$mode" = "integration" ];
then
  echo "Running Integration Tests"
  mvn verify -DskipUTs=true -B
else
  echo "Unknown mode $mode"
  exit 1;
fi

