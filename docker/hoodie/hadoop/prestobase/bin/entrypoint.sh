#!/bin/bash

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
