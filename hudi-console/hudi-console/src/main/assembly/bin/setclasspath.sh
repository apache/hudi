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

# -----------------------------------------------------------------------------
#  Set JAVA_HOME or JRE_HOME if not already set, ensure any provided settings
#  are valid and consistent with the selected start-up options.
# -----------------------------------------------------------------------------

# Make sure prerequisite environment variables are set
if [ -z "$JAVA_HOME" ] && [ -z "$JRE_HOME" ]; then
  # shellcheck disable=SC2154
  if $darwin; then
    # Bugzilla 54390
    if [ -x '/usr/libexec/java_home' ] ; then
      # shellcheck disable=SC2155
      export JAVA_HOME=$(/usr/libexec/java_home)
    # Bugzilla 37284 (reviewed).
    elif [ -d "/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home" ]; then
      export JAVA_HOME="/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home"
    fi
  else
    JAVA_PATH=$(which java 2>/dev/null)
    if [ "x$JAVA_PATH" != "x" ]; then
      JAVA_PATH=$(dirname "$JAVA_PATH" 2>/dev/null)
      JRE_HOME=$(dirname "$JAVA_PATH" 2>/dev/null)
    fi
    if [ "x$JRE_HOME" = "x" ]; then
      # XXX: Should we try other locations?
      if [ -x /usr/bin/java ]; then
        JRE_HOME=/usr
      fi
    fi
  fi
  if [ -z "$JAVA_HOME" ] && [ -z "$JRE_HOME" ]; then
    echo "Neither the JAVA_HOME nor the JRE_HOME environment variable is defined"
    echo "At least one of these environment variable is needed to run this program"
    exit 1
  fi
fi

if [ -z "$JRE_HOME" ]; then
  JRE_HOME="$JAVA_HOME"
fi

# Set standard commands for invoking Java, if not already set.
if [ -z "$_RUNJAVA" ]; then
  _RUNJAVA="$JRE_HOME"/bin/java
fi
