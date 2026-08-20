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

# The class the bundle exists to provide, and the one lost in the regression this guards against.
REQUIRED_CLASSES=(
  "org/apache/hudi/hadoop/HoodieParquetInputFormat.class"
  "org/apache/hudi/hadoop/realtime/HoodieParquetRealtimeInputFormat.class"
  "org/apache/hudi/common/table/HoodieTableMetaClient.class"
)

# The bundle shades all of hudi-hadoop-mr and hudi-hadoop-common; it carried 109 such entries when this
# check was written. A floor rather than an exact count, so ordinary additions do not fail the build while
# a collapse to zero still does.
MIN_HADOOP_ENTRIES=100

echo "::warning::validate_presto_bundle.sh validating $(basename "$JAR")"

listing=$(unzip -l "$JAR")

for class in "${REQUIRED_CLASSES[@]}"; do
  if ! echo "$listing" | grep -q " $class$"; then
    echo "::error::$class is missing from $(basename "$JAR"). An artifactSet include probably matched no"
    echo "::error::artifact. Check that the bundle declares the modules it shades, and that it was built"
    echo "::error::without -am so bundle dependencies resolve from the repository."
    exit 1
  fi
  echo "  found $class"
done

hadoop_entries=$(echo "$listing" | grep -c "org/apache/hudi/hadoop/" || true)
if [ "$hadoop_entries" -lt "$MIN_HADOOP_ENTRIES" ]; then
  echo "::error::$(basename "$JAR") has only $hadoop_entries org/apache/hudi/hadoop/** entries,"
  echo "::error::expected at least $MIN_HADOOP_ENTRIES. The bundle has shrunk; see HUDI #19433."
  exit 1
fi
echo "  $hadoop_entries org/apache/hudi/hadoop/** entries (floor $MIN_HADOOP_ENTRIES)"

echo "::warning::validate_presto_bundle.sh validation of $(basename "$JAR") was successful."
