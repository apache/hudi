#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

wait_until() {
    local hostname=${1?}
    local port=${2?}
    local retry=${3:-100}
    local sleep_secs=${4:-2}
    
    local address_up=0
    
    while [ ${retry} -gt 0 ] ; do
        echo  "Waiting until ${hostname}:${port} is up ... with retry count: ${retry}"
        if nc -z ${hostname} ${port}; then
            address_up=1
            break
        fi        
        retry=$((retry-1))
        sleep ${sleep_secs}
    done 
    
    if [ $address_up -eq 0 ]; then
        echo "GIVE UP waiting until ${hostname}:${port} is up! "
        exit 1
    fi       
}

if [ ! -e ${PRESTO_LOG_DIR}/node.id ]; then
    cat /proc/sys/kernel/random/uuid > ${PRESTO_LOG_DIR}/node.id
fi

export PRESTO_NODE_ID=$(cat ${PRESTO_LOG_DIR}/node.id)

# apply template
for template in $(ls ${PRESTO_CONF_DIR}/*.mustache)
do
    conf_file=${template%.mustache}
    cat ${conf_file}.mustache | mustache.sh > ${conf_file}
done

# Copy the presto bundle at run time so that locally built bundle overrides the one that is present in the image
cp ${HUDI_PRESTO_BUNDLE} ${PRESTO_HOME}/plugin/hive-hadoop2/

case "$1" in
    "coordinator" | "worker" )
        server_role="$1"
        shift
        exec gosu presto launcher --config=${PRESTO_CONF_DIR}/${server_role}.properties "$@" run
        ;;
    *)
        ;; 
esac
 
exec "$@"
