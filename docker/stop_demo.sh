#!/bin/bash

# set up root directory
WS_ROOT=`dirname $PWD`
# shut down cluster
HUDI_WS=${WS_ROOT} docker-compose -f compose/docker-compose_hadoop284_hive233_spark240.yml down

# remove host mount directory
rm -rf /tmp/hadoop_data
rm -rf /tmp/hadoop_name
