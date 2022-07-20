# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/bin/bash

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Preserve the calling directory
_CALLING_DIR="$(pwd)"

#########################
# The command line help #
#########################
usage() {
    echo "Usage: $0"
    echo "   --spark-command, prints the spark command"
    echo "   -h | --hadoop, hadoop-version"
    echo "   -s | --spark, spark version"
    echo "   -p | --parquet, parquet version"
    echo "   -a | --avro, avro version"
    echo "   -i | --hive, hive version"
    echo "   -l | --scala, scala version"
    exit 1
}

get_spark_command() {
  if [ -z "$scala" ]
  then
    scala="2.11"
  else
    scala=$scala
  fi
  echo "spark-submit --packages org.apache.spark:spark-avro_${scala}:3.1.3 \
          --master $0 \
          --deploy-mode $1 \
          --properties-file $2 \
          --class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob \
          `ls target/hudi-integ-test-*-SNAPSHOT.jar` \
          --source-class $3 \
          --source-ordering-field $4 \
          --input-base-path $5 \
          --target-base-path $6 \
          --target-table $7 \
          --props $8 \
          --storage-type $9 \
          --payload-class "${10}" \
          --workload-yaml-path "${11}" \
          --input-file-size "${12}" \
          --<use-deltastreamer>"
}

case "$1" in
   --help)
       usage
       exit 0
       ;;
esac

case "$1" in
   --spark-command)
       get_spark_command
       exit 0
       ;;
esac

while getopts ":h:s:p:a:i:l:-:" opt; do
  case $opt in
    h) hadoop="$OPTARG"
    printf "Argument hadoop is %s\n" "$hadoop"
    ;;
    s) spark="$OPTARG"
    printf "Argument spark is %s\n" "$spark"
    ;;
    p) parquet="$OPTARG"
    printf "Argument parquet is %s\n" "$parquet"
    ;;
    a) avro="$OPTARG"
    printf "Argument avro is %s\n" "$avro"
    ;;
    i) hive="$OPTARG"
    printf "Argument hive is %s\n" "$hive"
    ;;
    l) scala="$OPTARG"
    printf "Argument scala is %s\n" "$scala"
    ;;
    -)
      case "$OPTARG" in
        hadoop)
          hadoop="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument hadoop is %s\n" "$hadoop"
          ;;
        spark)
          spark="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument spark is %s\n" "$spark"
          ;;
        parquet)
          parquet="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument parquet is %s\n" "$parquet"
          ;;
        avro)
          avro="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument avro is %s\n" "$avro"
          ;;
        hive)
          hive="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument hive is %s\n" "$hive"
          ;;
        scala)
          scala="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
          printf "Argument scala is %s\n" "$scala"
          ;;
        *) echo "Invalid option --$OPTARG" >&2
          ;;
    esac ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done


get_versions () {
  base_command=''
  if [ -z "$hadoop" ]
   then
    base_command=$base_command
  else
    base_command+=' -Dhadoop.version='$hadoop
  fi

  if [ -z "$hive" ]
  then
    base_command=$base_command
  else
    base_command+=' -Dhive.version='$hive
  fi

  if [ -z "$scala" ]
  then
    base_command=$base_command
  else
    base_command+=' -Dscala-'$scala
  fi
  echo $base_command
}

versions=$(get_versions)

final_command='mvn clean install -DskipTests '$versions
printf "Final command $final_command \n"

# change to the project root directory to run maven command
move_to_root='cd ..'
$move_to_root && $final_command

# change back to original working directory
cd $_CALLING_DIR

printf "A sample spark command to start the integration suite \n"
get_spark_command