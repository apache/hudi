#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

state=${1:-"start"}
state=$(echo "$state" | tr '[:upper:]' '[:lower:]')

# ----------------------------------------------------------
# Function to determine which docker compose command to use
# ----------------------------------------------------------
get_docker_compose_cmd() {
    if docker compose version &>/dev/null; then
        echo "docker compose"
    elif docker-compose version &>/dev/null; then
        echo "docker-compose"
    else
        echo "ERROR: Neither 'docker compose' nor 'docker-compose' is installed or available in PATH." >&2
        exit 1
    fi
}

# Detect and assign the correct compose command
DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)

case "$state" in
  start)
    $DOCKER_COMPOSE_CMD up -d
    ;;
  stop)
    $DOCKER_COMPOSE_CMD down
    ;;
  restart)
    $DOCKER_COMPOSE_CMD down
    $DOCKER_COMPOSE_CMD up -d --build
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}"
    exit 1
esac
