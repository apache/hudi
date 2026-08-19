#!/bin/bash
#
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
#

##
# Validates the native spark bundle by writing and querying Hudi tables with Comet enabled.
#
# Must run on glibc with Java 17. Comet's libcomet.so is glibc-linked and its bytecode is class
# file version 61, so this cannot run in the Alpine based bundle-validation image; the caller is
# responsible for providing a suitable environment.
#
# env vars:
#   SPARK_HOME:        path to the spark directory
#   NATIVE_BUNDLE_JAR: path to the hudi native spark bundle jar
##
set -o errexit
set -o nounset

WORKDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
outputDir=/tmp/native-spark-bundle
rm -rf $outputDir

echo "::warning::validating native spark bundle with $NATIVE_BUNDLE_JAR"
$SPARK_HOME/bin/spark-shell --jars "$NATIVE_BUNDLE_JAR" \
  --conf 'spark.plugins=org.apache.spark.CometPlugin' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.comet.CometSparkSessionExtensions' \
  --conf 'spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager' \
  --conf 'spark.comet.enabled=true' \
  --conf 'spark.comet.exec.enabled=true' \
  --conf 'spark.comet.convert.parquet.enabled=true' \
  --conf 'spark.comet.explain.fallback.enabled=true' \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  < "$WORKDIR/validate.scala"

numRows=$(cat $outputDir/count/part-*)
if [ "$numRows" -ne 3 ]; then
    echo "::error::native spark bundle query returned $numRows rows, expected 3"
    exit 1
fi

# Comet declines what it cannot accelerate and hands it back to Spark, so a correct row count on
# its own would still pass with a mis-relocated Comet or a libcomet.so that failed to load.
# Require the join itself to be native.
if ! grep -q 'CometSortMergeJoin' $outputDir/plan/part-*; then
    echo "::error::join over Hudi tables was not executed natively by Comet"
    cat $outputDir/plan/part-*
    exit 1
fi
echo "::warning::native spark bundle validation was successful"
