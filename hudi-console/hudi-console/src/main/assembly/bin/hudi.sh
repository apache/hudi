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
#
# -----------------------------------------------------------------------------
# Control Script for the Hudi Server
#
# Environment Variable Prerequisites
#
#   APP_HOME   May point at your Hudi "build" directory.
#
#   APP_BASE   (Optional) Base directory for resolving dynamic portions
#                   of a Hudi installation.  If not present, resolves to
#                   the same directory that APP_HOME points to.
#
#   APP_CONF    (Optional) config path
#
#   APP_PID    (Optional) Path of the file which should contains the pid
#                   of the Hudi startup java process, when start (fork) is
#                   used
# -----------------------------------------------------------------------------

# Bugzilla 37848: When no TTY is available, don't output to console
have_tty=0
# shellcheck disable=SC2006
if [[ "`tty`" != "not a tty" ]]; then
    have_tty=1
fi

# Bugzilla 37848: When no TTY is available, don't output to console
have_tty=0
# shellcheck disable=SC2006
if [[ "`tty`" != "not a tty" ]]; then
    have_tty=1
fi

 # Only use colors if connected to a terminal
if [[ ${have_tty} -eq 1 ]]; then
  PRIMARY=$(printf '\033[38;5;082m')
  RED=$(printf '\033[31m')
  GREEN=$(printf '\033[32m')
  YELLOW=$(printf '\033[33m')
  BLUE=$(printf '\033[34m')
  BOLD=$(printf '\033[1m')
  RESET=$(printf '\033[0m')
else
  PRIMARY=""
  RED=""
  GREEN=""
  YELLOW=""
  BLUE=""
  BOLD=""
  RESET=""
fi

echo_r () {
    # Color red: Error, Failed
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sHUDI%s] %s$1%s\n"  $BLUE $RESET $RED $RESET
}

echo_g () {
    # Color green: Success
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sHUDI%s] %s$1%s\n"  $BLUE $RESET $GREEN $RESET
}

echo_y () {
    # Color yellow: Warning
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sHUDI%s] %s$1%s\n"  $BLUE $RESET $YELLOW $RESET
}

echo_w () {
    # Color yellow: White
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sHUDI%s] %s$1%s\n"  $BLUE $RESET $WHITE $RESET
}

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false
os400=false
# shellcheck disable=SC2006
case "`uname`" in
CYGWIN*) cygwin=true;;
OS400*) os400=true;;
esac

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

# shellcheck disable=SC2006
# shellcheck disable=SC2164
APP_HOME=`cd "$PRG_DIR/.." >/dev/null; pwd`
APP_BASE="$APP_HOME"
APP_CONF="$APP_BASE"/conf
APP_LIB="$APP_BASE"/lib
APP_LOG="$APP_BASE"/logs
APP_PID="$APP_BASE"/.pid
APP_OUT="$APP_LOG"/hudi.out
# shellcheck disable=SC2034
APP_TMPDIR="$APP_BASE"/temp

# Ensure that any user defined CLASSPATH variables are not used on startup,
# but allow them to be specified in setenv.sh, in rare case when it is needed.
CLASSPATH=

if [[ -r "$APP_BASE/bin/setenv.sh" ]]; then
  # shellcheck disable=SC1090
  . "$APP_BASE/bin/setenv.sh"
elif [[ -r "$APP_HOME/bin/setenv.sh" ]]; then
  # shellcheck disable=SC1090
  . "$APP_HOME/bin/setenv.sh"
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if ${cygwin}; then
  # shellcheck disable=SC2006
  [[ -n "$JAVA_HOME" ]] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  # shellcheck disable=SC2006
  [[ -n "$JRE_HOME" ]] && JRE_HOME=`cygpath --unix "$JRE_HOME"`
  # shellcheck disable=SC2006
  [[ -n "$APP_HOME" ]] && APP_HOME=`cygpath --unix "$APP_HOME"`
  # shellcheck disable=SC2006
  [[ -n "$APP_BASE" ]] && APP_BASE=`cygpath --unix "$APP_BASE"`
  # shellcheck disable=SC2006
  [[ -n "$CLASSPATH" ]] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# Ensure that neither APP_HOME nor APP_BASE contains a colon
# as this is used as the separator in the classpath and Java provides no
# mechanism for escaping if the same character appears in the path.
case ${APP_HOME} in
  *:*) echo "Using APP_HOME:   $APP_HOME";
       echo "Unable to start as APP_HOME contains a colon (:) character";
       exit 1;
esac
case ${APP_BASE} in
  *:*) echo "Using APP_BASE:   $APP_BASE";
       echo "Unable to start as APP_BASE contains a colon (:) character";
       exit 1;
esac

# For OS400
if ${os400}; then
  # Set job priority to standard for interactive (interactive - 6) by using
  # the interactive priority - 6, the helper threads that respond to requests
  # will be running at the same priority as interactive jobs.
  COMMAND='chgjob job('${JOBNAME}') runpty(6)'
  system "${COMMAND}"

  # Enable multi threading
  export QIBM_MULTI_THREADED=Y
fi

# Get standard Java environment variables
if ${os400}; then
  # -r will Only work on the os400 if the files are:
  # 1. owned by the user
  # 2. owned by the PRIMARY group of the user
  # this will not work if the user belongs in secondary groups
  # shellcheck disable=SC1090
  . "$APP_HOME"/bin/setclasspath.sh
else
  if [[ -r "$APP_HOME"/bin/setclasspath.sh ]]; then
    # shellcheck disable=SC1090
    . "$APP_HOME"/bin/setclasspath.sh
  else
    echo "Cannot find $APP_HOME/bin/setclasspath.sh"
    echo "This file is needed to run this program"
    exit 1
  fi
fi

# Add on extra jar files to CLASSPATH
# shellcheck disable=SC2236
if [[ ! -z "$CLASSPATH" ]] ; then
  CLASSPATH="$CLASSPATH":
fi
CLASSPATH="$CLASSPATH"

# For Cygwin, switch paths to Windows format before running java
if ${cygwin}; then
  # shellcheck disable=SC2006
  JAVA_HOME=`cygpath --absolute --windows "$JAVA_HOME"`
  # shellcheck disable=SC2006
  JRE_HOME=`cygpath --absolute --windows "$JRE_HOME"`
  # shellcheck disable=SC2006
  APP_HOME=`cygpath --absolute --windows "$APP_HOME"`
  # shellcheck disable=SC2006
  APP_BASE=`cygpath --absolute --windows "$APP_BASE"`
  # shellcheck disable=SC2006
  CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
fi

if [ -z "$USE_NOHUP" ]; then
  if $hpux; then
    USE_NOHUP="true"
  else
    USE_NOHUP="false"
  fi
fi
unset NOHUP
if [ "$USE_NOHUP" = "true" ]; then
  NOHUP="nohup"
fi

# ----- Execute The Requested Command -----------------------------------------

print_logo() {
  printf '\n'
  printf '      %s     __  __     __  __     _____     __                      %s\n'           $PRIMARY $RESET
  printf '      %s    /\ \_\ \   /\ \/\ \   /\  __-.  /\ \                     %s\n'           $PRIMARY $RESET
  printf '      %s    \ \  __ \  \ \ \_\ \  \ \ \/\ \ \ \ \                    %s\n'           $PRIMARY $RESET
  printf '      %s     \ \_\ \_\  \ \_____\  \ \____-  \ \_\                   %s\n'           $PRIMARY $RESET
  printf '      %s      \/_/\/_/   \/_____/   \/____/   \/_/                   %s\n\n'           $PRIMARY $RESET
  printf '      %s   Version:  0.13.0 %s\n'                                                        $BLUE   $RESET
  printf '      %s   WebSite:  https://hudi.apache.org%s\n'                                        $BLUE   $RESET
  printf '      %s   GitHub :  http://github.com/apache/hudi%s\n\n'                                $BLUE   $RESET
  printf '      %s   ──────── Upserts, Deletes And Incremental Processing on Big Data.%s\n\n'      $PRIMARY  $RESET
}

# shellcheck disable=SC2120
running() {
  if [ -f "$APP_PID" ]; then
    if [ -s "$APP_PID" ]; then
        # shellcheck disable=SC2046
        # shellcheck disable=SC2006
        kill -0 `cat "$APP_PID"` >/dev/null 2>&1
        # shellcheck disable=SC2181
        if [ $? -eq 0 ]; then
          return 1
        else
          return 0
        fi
    else
      return 0
    fi
  else
    return 0
  fi
}

# shellcheck disable=SC2120
start() {
  running
  # shellcheck disable=SC2181
  if [ $? -eq "1" ]; then
    # shellcheck disable=SC2006
    echo_r "Hudi is already running pid: `cat "$APP_PID"`"
    exit 1
  fi

  # Bugzilla 37848: only output this if we have a TTY
  if [[ ${have_tty} -eq 1 ]]; then
    echo_w "Using APP_BASE:   $APP_BASE"
    echo_w "Using APP_HOME:   $APP_HOME"
    if [[ "$1" = "debug" ]] ; then
      echo_w "Using JAVA_HOME:   $JAVA_HOME"
    else
      echo_w "Using JRE_HOME:   $JRE_HOME"
    fi
    echo_w "Using APP_PID:   $APP_PID"
  fi

  PROPER="${APP_CONF}/application.yml"
  if [[ ! -f "$PROPER" ]] ; then
    PROPER="${APP_CONF}/application.properties"
    if [[ ! -f "$PROPER" ]] ; then
      echo_r "Usage: properties file (application.properties|application.yml) not found! ";
    else
      echo_g "Usage: properties file:application.properties ";
    fi
  else
    echo_g "Usage: properties file:application.yml ";
  fi

  if [ "${HADOOP_HOME}"x == ""x ]; then
    echo_y "WARN: HADOOP_HOME is undefined on your system env,please check it."
  else
    echo_w "Using HADOOP_HOME:   ${HADOOP_HOME}"
  fi

  #
  # classpath options:
  # 1): java env (lib and jre/lib)
  # 2): HUDI jar
  # 3): hadoop conf
  # shellcheck disable=SC2091
  APP_CLASSPATH=".:${JAVA_HOME}/lib:${JAVA_HOME}/jre/lib"
  # shellcheck disable=SC2206
  # shellcheck disable=SC2010
  JARS=$(ls "$APP_LIB"/*.jar)
  # shellcheck disable=SC2128
  for jar in $JARS;do
    APP_CLASSPATH=$APP_CLASSPATH:$jar
  done

  if [[ -n "${HADOOP_CONF_DIR}" ]] && [[ -d "${HADOOP_CONF_DIR}" ]]; then
    echo_w "Using HADOOP_CONF_DIR:   ${HADOOP_CONF_DIR}"
    APP_CLASSPATH+=":${HADOOP_CONF_DIR}"
  else
    APP_CLASSPATH+=":${HADOOP_HOME}/etc/hadoop"
  fi

  # shellcheck disable=SC2034
  # shellcheck disable=SC2006
  vmOption=`$_RUNJAVA -cp "$APP_CLASSPATH"`

  JAVA_OPTS="""
  $vmOption
  -ea
  -server
  -Xms1024m
  -Xmx1024m
  -Xmn256m
  -XX:NewSize=100m
  -XX:+UseConcMarkSweepGC
  -XX:CMSInitiatingOccupancyFraction=70
  -XX:ThreadStackSize=512
  -Xloggc:${APP_HOME}/logs/gc.log
  $DEBUG_OPTS
  """

  eval $NOHUP "\"$_RUNJAVA\"" $JAVA_OPTS \
    -classpath "\"$APP_CLASSPATH\"" \
    -Dapp.home="\"${APP_HOME}\"" \
    -Dlogging.config="\"${APP_CONF}\"/logback-spring.xml" \
    -Dspring.config.location="\"${PROPER}\"" \
    -Djava.io.tmpdir="\"$APP_TMPDIR\"" \
    -Dpid="\"${APP_PID}\"" \
    org.apache.hudi.console.HudiConsoleBootstrap >> "$APP_OUT" 2>&1 "&"

    local PID=$!

    # Add to pid file if successful start
    if [[ ${PID} =~ ${IS_NUMBER} ]] && kill -0 $PID > /dev/null 2>&1 ; then
      echo $PID > "$APP_PID"
      echo_g "Hudi Console start successful. pid: `cat "$APP_PID"`"
    else
      echo_r "Hudi Console  start failed."
      exit 1
    fi
}

debug() {
  if [ ! -n "$DEBUG_PORT" ]; then
    echo_r "If start with debug mode,Please fill in the debug port like: bash hudi.sh debug 10002 "
  else
    DEBUG_OPTS="""
    -Xdebug  -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT
    """
    start
  fi
}

# shellcheck disable=SC2120
stop() {
  running
  # shellcheck disable=SC2181
  if [ $? -eq "0" ]; then
    echo_r "Hudi is not running."
    exit 1
  fi

  shift

  local SLEEP=5
  if [ ! -z "$1" ]; then
    echo $1 | grep "[^0-9]" >/dev/null 2>&1
    if [ $? -gt 0 ]; then
      SLEEP=$1
      shift
    fi
  fi

  local FORCE=0
  if [ "$1" = "-force" ]; then
    shift
    FORCE=1
  fi

  local STOPPED=0
  # shellcheck disable=SC2236
  if [ -f "$APP_PID" ]; then
    if [ -s "$APP_PID" ]; then
      # shellcheck disable=SC2046
      # shellcheck disable=SC2006
      kill -0 `cat "$APP_PID"` >/dev/null 2>&1
      # shellcheck disable=SC2181
      if [ $? -gt 0 ]; then
        echo "PID file found but either no matching process was found or the current user does not have permission to stop the process. Stop aborted."
        exit 1
      else
        # shellcheck disable=SC2046
        # shellcheck disable=SC2006
        kill -15 `cat "$APP_PID"` >/dev/null 2>&1
        if [ -f "$APP_PID" ]; then
          while [ $SLEEP -ge 0 ]; do
             if [ -f "$APP_PID" ]; then
               # shellcheck disable=SC2046
               # shellcheck disable=SC2006
               kill -0 `cat "$APP_PID"` >/dev/null 2>&1
               if [ $? -gt 0 ]; then
                 rm -f "$APP_PID" >/dev/null 2>&1
                 if [ $? != 0 ]; then
                   if [ -w "$APP_PID" ]; then
                     cat /dev/null > "$APP_PID"
                     # If Hudi has stopped don't try and force a stop with an empty PID file
                     FORCE=0
                   else
                     echo "The PID file could not be removed or cleared."
                   fi
                 fi
                 STOPPED=1
                 break
               fi
             else
               STOPPED=1
               break
             fi
             SLEEP=`expr $SLEEP - 1 `
          done

          #stop failed.normal kill failed? Try a force kill.
          if [ -f "$APP_PID" ]; then
            if [ -s "$APP_PID" ]; then
              # shellcheck disable=SC2046
              kill -0 `cat "$APP_PID"` >/dev/null 2>&1
              if [ $? -eq 0 ]; then
                FORCE=1
              fi
            fi
          fi

          if [ $FORCE -eq 1 ]; then
            KILL_SLEEP_INTERVAL=5
            if [ -f "$APP_PID" ]; then
              # shellcheck disable=SC2006
              PID=`cat "$APP_PID"`
              echo_y "Killing Hudi with the PID: $PID"
              kill -9 "$PID" >/dev/null 2>&1
              while [ $KILL_SLEEP_INTERVAL -ge 0 ]; do
                kill -0 `cat "$APP_PID"` >/dev/null 2>&1
                if [ $? -gt 0 ]; then
                  rm -f "$APP_PID" >/dev/null 2>&1
                  if [ $? != 0 ]; then
                    if [ -w "$APP_PID" ]; then
                      cat /dev/null > "$APP_PID"
                    else
                      echo_r "The PID file could not be removed."
                    fi
                  fi
                  echo_y "The Hudi process has been killed."
                  break
                fi
                if [ $KILL_SLEEP_INTERVAL -gt 0 ]; then
                  sleep 1
                fi
                KILL_SLEEP_INTERVAL=`expr $KILL_SLEEP_INTERVAL - 1 `
              done
              if [ $KILL_SLEEP_INTERVAL -lt 0 ]; then
                echo "Hudi has not been killed completely yet. The process might be waiting on some system call or might be UNINTERRUPTIBLE."
              fi
            fi
          fi
        else
          STOPPED=1
        fi
      fi
    else
      echo "PID file is empty and has been ignored."
      exit 1
    fi
  else
    echo "\$APP_PID was set but the specified file does not exist. Is Hudi running? Stop aborted."
    exit 1
  fi

  if [ $STOPPED -eq 1 ]; then
     echo_r "Hudi stopped."
  fi

}

status() {
  running
  # shellcheck disable=SC2181
  if [ $? -eq "1" ]; then
    # shellcheck disable=SC2006
    echo_g "Hudi is running pid is: `cat "$APP_PID"`"
  else
    echo_r "Hudi is not running"
  fi
}

restart() {
  # shellcheck disable=SC2119
  stop
  # shellcheck disable=SC2119
  start
}

main() {
  print_logo
  case "$1" in
    "debug")
        DEBUG_PORT=$2
        debug
        ;;
    "start")
        start
        ;;
    "stop")
        stop
        ;;
    "status")
        status
        ;;
    "restart")
        restart
        ;;
    *)
        echo_r "Unknown command: $1"
        echo_w "Usage: hudi.sh ( commands ... )"
        echo_w "commands:"
        echo_w "  start \$conf               Start Hudi with application config."
        echo_w "  stop n -force             Stop Hudi, wait up to n seconds and then use kill -KILL if still running"
        echo_w "  status                    Hudi status"
        echo_w "  debug                     Hudi start with debug mode,start debug mode, like: bash hudi.sh debug 10002"
        echo_w "  restart \$conf             restart Hudi with application config."
        exit 0
        ;;
  esac
}

main "$@"
