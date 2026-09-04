#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Asserts that hudi-presto-bundle contains the classes it exists to provide.
#
# maven-shade-plugin does not fail when an artifactSet include matches nothing, so a bundle can silently
# ship without the classes it is supposed to carry and the build still succeeds. That is exactly what
# happened before HUDI #19433: hudi-presto-bundle went from 109 org/apache/hudi/hadoop/** entries to 0,
# losing HoodieParquetInputFormat, and no job noticed.
#
# The caller must build the bundle WITHOUT -am, so its bundle dependencies resolve from the repository
# rather than from the reactor. With -am, Maven's ReactorReader serves the effective model instead of the
# published dependency-reduced POM, and the resolution path that actually broke is never exercised.
#
# Usage: validate_presto_bundle.sh <path-to-hudi-presto-bundle.jar>

set -e

JAR=$1

if [ -z "$JAR" ]; then
  echo "::error::usage: $0 <path-to-hudi-presto-bundle.jar>"
  exit 1
fi

if [ ! -f "$JAR" ]; then
  echo "::error::presto bundle jar not found: $JAR"
  exit 1
fi

# Only the main artifact carries the shaded classes. The sources and javadoc jars do not, and a glob picks
# them up ahead of it because "-" sorts before "." - which is how this script first failed in CI, reporting
# a missing class against hudi-presto-bundle-<version>-javadoc.jar. Refuse them rather than mislead.
case "$(basename "$JAR")" in
  *-sources.jar|*-javadoc.jar|*-tests.jar)
    echo "::error::$(basename "$JAR") is not the main artifact. Pass"
    echo "::error::packaging/hudi-presto-bundle/target/hudi-presto-bundle-<version>.jar instead."
    exit 1
    ;;
esac

# One sentinel per artifactSet include, so the loss of any single one is caught by name rather than only
# by the floor below. Seven of the eleven third-party includes are reached only transitively, which is the
# HUDI #19433 shape, and shade does not fail when an include matches nothing. Every path here was checked
# against a bundle built the way this step builds it.
#
# Hudi modules:
#   HoodieParquetInputFormat          hudi-hadoop-mr, the class lost in #19433
#   HoodieParquetRealtimeInputFormat  hudi-hadoop-mr, the MOR snapshot path
#   HoodieROTablePathFilter           hudi-hadoop-mr, the COW/MOR read-optimized path Presto uses (rfc-44)
#   HadoopFSUtils                     hudi-hadoop-common, the other include #19433 broke
#   HoodieTableMetaClient             hudi-common
# Relocated third-party, all transitive:
#   Kryo, Log, ObjenesisStd           kryo-shaded, minlog, objenesis
#   Schema                            avro
#   IOUtils, StringUtils              commons-io, commons-lang3
#   Message                           protobuf-java
#   ClassLayout                       jol-core; its absence was #6839 (NoClassDefFoundError: GraphLayout)
# Bundled unrelocated by design:
#   Caffeine                          caffeine
#
# Two includes are deliberately not covered. parquet-avro's classes land under the bootstrap shade prefix
# alongside protobuf, which Message already guards. com.yammer.metrics:metrics-core contributes 0 entries
# to the jar - Hudi is on dropwizard - so it is a dead include with nothing to assert; removing it belongs
# with the rest of #19490's cleanup rather than here.
REQUIRED_CLASSES=(
  "org/apache/hudi/hadoop/HoodieParquetInputFormat.class"
  "org/apache/hudi/hadoop/realtime/HoodieParquetRealtimeInputFormat.class"
  "org/apache/hudi/hadoop/HoodieROTablePathFilter.class"
  "org/apache/hudi/hadoop/fs/HadoopFSUtils.class"
  "org/apache/hudi/common/table/HoodieTableMetaClient.class"
  "org/apache/hudi/com/esotericsoftware/kryo/Kryo.class"
  "org/apache/hudi/com/esotericsoftware/minlog/Log.class"
  "org/apache/hudi/org/objenesis/ObjenesisStd.class"
  "org/apache/hudi/org/apache/avro/Schema.class"
  "org/apache/hudi/org/apache/commons/io/IOUtils.class"
  "org/apache/hudi/org/apache/commons/lang3/StringUtils.class"
  "org/apache/hudi/com/google/protobuf/Message.class"
  "org/apache/hudi/org/openjdk/jol/info/ClassLayout.class"
  "com/github/benmanes/caffeine/cache/Caffeine.class"
)

# Packages that must never appear unrelocated. Shading them under org/apache/hudi/ is the point of the
# relocation config, and a lost relocation leaks them at the top level where they collide with the
# engine's own copies: 623 org/codehaus/jackson/** classes shipped that way before #19490, and #19814 was
# the same shape for avro on the datahub bundle. Matched with a leading space so the relocated
# org/apache/hudi/org/apache/avro/** entries do not count.
#
# org/apache/parquet/ cannot join this list: hudi-hadoop-common legitimately ships SchemaRepair and
# ParquetConfiguration in that package, so it is 5 entries on a clean build rather than 0.
FORBIDDEN_UNRELOCATED=(
  "org/apache/avro/"
  "org/codehaus/jackson/"
)

# The bundle shades all of hudi-hadoop-mr and hudi-hadoop-common; it carried 102 such classes when this
# check was written. A floor rather than an exact count, so ordinary additions do not fail the build while
# a collapse still does. Counted on classes alone: including directory entries inflates the total by 8 and
# would let the loss of hudi-hadoop-common (14 classes) slip under a floor set against the larger number.
MIN_HADOOP_CLASSES=100

echo "::warning::validate_presto_bundle.sh validating $(basename "$JAR")"

listing=$(unzip -l "$JAR")

# A here-string rather than a pipe: grep -q exits on the first match and closes the pipe, which makes
# echo report "write error: Broken pipe" on every green run.
for class in "${REQUIRED_CLASSES[@]}"; do
  if ! grep -q " $class$" <<< "$listing"; then
    echo "::error::$class is missing from $(basename "$JAR"). An artifactSet include probably matched no"
    echo "::error::artifact. Check that the bundle declares the modules it shades, and that it was built"
    echo "::error::without -am so bundle dependencies resolve from the repository."
    exit 1
  fi
  echo "  found $class"
done

for package in "${FORBIDDEN_UNRELOCATED[@]}"; do
  leaked=$(grep -c " $package" <<< "$listing" || true)
  if [ "$leaked" -ne 0 ]; then
    echo "::error::$(basename "$JAR") ships $leaked unrelocated $package** entries, expected none."
    echo "::error::A relocation has been lost, so these collide with the engine's own copies at runtime."
    echo "::error::See HUDI #19490 (jackson) and #19814 (avro)."
    exit 1
  fi
  echo "  no unrelocated $package** entries"
done

hadoop_classes=$(grep -c "org/apache/hudi/hadoop/.*\.class$" <<< "$listing" || true)
if [ "$hadoop_classes" -lt "$MIN_HADOOP_CLASSES" ]; then
  echo "::error::$(basename "$JAR") has only $hadoop_classes org/apache/hudi/hadoop/** classes,"
  echo "::error::expected at least $MIN_HADOOP_CLASSES. The bundle has shrunk; see HUDI #19433."
  exit 1
fi
echo "  $hadoop_classes org/apache/hudi/hadoop/** classes (floor $MIN_HADOOP_CLASSES)"

echo "::warning::validate_presto_bundle.sh validation of $(basename "$JAR") was successful."
