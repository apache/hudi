#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# resolve links - $0 may be a softlink
PRG="$0"

while [[ -h "$PRG" ]]; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
# shellcheck disable=SC2006
PRG_DIR=`dirname "$PRG"`
EXECUTABLE=hudi.sh

# Check that target executable exists
if $os400; then
  # -x will Only work on the os400 if the files are:
  # 1. owned by the user
  # 2. owned by the PRIMARY group of the user
  # this will not work if the user belongs in secondary groups
  eval
else
  if [ ! -x "$PRG_DIR"/"$EXECUTABLE" ]; then
    echo "Cannot find $PRG_DIR/$EXECUTABLE"
    echo "The file is absent or does not have execute permission"
    echo "This file is needed to run this program"
    exit 1
  fi
fi

exec "$PRG_DIR"/"$EXECUTABLE" stop "$@"

