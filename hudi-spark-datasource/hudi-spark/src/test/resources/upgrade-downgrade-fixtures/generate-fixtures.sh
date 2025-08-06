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

# Script to generate Hudi MOR table fixtures for different versions
# Used by TestUpgradeDowngradeFixtures for integration testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/mor-tables"

echo "Generating Hudi upgrade/downgrade test fixtures..."
echo "Fixtures directory: $FIXTURES_DIR"

# Parse command line arguments
REQUESTED_VERSIONS=""
HUDI_BUNDLE_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            REQUESTED_VERSIONS="$2"
            shift 2
            ;;
        --hudi-bundle-path)
            HUDI_BUNDLE_PATH="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--version <version_list>] [--hudi-bundle-path <path>]"
            echo "  --version <version_list>          Comma-separated list of table versions to generate (e.g., 4,5,6)"
            echo "  --hudi-bundle-path <path>         Path to locally built Hudi bundle JAR (required for version 9)"
            echo ""
            echo "Examples:"
            echo "  $0                                           # Generate all versions (except 9)"
            echo "  $0 --version 4,5                             # Generate versions 4 and 5"
            echo "  $0 --version 9 --hudi-bundle-path /path/to/hudi-spark3.5-bundle_2.12-1.1.0-SNAPSHOT.jar"
            echo ""
            echo "Note: Version 9 requires a locally built Hudi bundle from master branch"
            exit 1
            ;;
    esac
done

# Convert comma-separated versions to array
if [ -n "$REQUESTED_VERSIONS" ]; then
    IFS=',' read -ra VERSION_ARRAY <<< "$REQUESTED_VERSIONS"
    echo "INFO: Generating specific versions: ${VERSION_ARRAY[@]}"
else
    echo "INFO: Generating all fixture versions"
fi

# Function to check if a version should be generated
should_generate_version() {
    local version=$1
    # If no specific versions requested, generate all
    if [ -z "$REQUESTED_VERSIONS" ]; then
        return 0
    fi
    # Check if version is in the requested array
    for requested in "${VERSION_ARRAY[@]}"; do
        if [ "$requested" = "$version" ]; then
            return 0
        fi
    done
    return 1
}

# Validate version 9 requirements
if should_generate_version "9"; then
    if [ -z "$HUDI_BUNDLE_PATH" ]; then
        echo "ERROR: Version 9 requires --hudi-bundle-path argument"
        echo ""
        echo "To generate table version 9, you need to:"
        echo "1. Build Hudi from master branch:"
        echo "   cd <hudi-repo>"
        echo "   mvn clean install -DskipTests -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am"
        echo ""
        echo "2. Provide the path to the built bundle:"
        echo "   $0 --version 9 --hudi-bundle-path <hudi-repo>/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.1.0-SNAPSHOT.jar"
        exit 1
    fi
    
    if [ ! -f "$HUDI_BUNDLE_PATH" ]; then
        echo "ERROR: Hudi bundle not found at: $HUDI_BUNDLE_PATH"
        exit 1
    fi
    
    echo "Using local Hudi bundle: $HUDI_BUNDLE_PATH"
fi

# Function to ensure Spark binary is available locally
ensure_spark_binary() {
    local spark_version=$1
    
    # Determine exact Spark version and tarball name
    case "$spark_version" in
        "3.2")
            local spark_full_version="3.2.4"
            local spark_tarball="spark-3.2.4-bin-hadoop3.2.tgz"
            local extracted_dirname="spark-3.2.4-bin-hadoop3.2"
            ;;
        "3.3")
            local spark_full_version="3.3.4"
            local spark_tarball="spark-3.3.4-bin-hadoop3.tgz"
            local extracted_dirname="spark-3.3.4-bin-hadoop3"
            ;;
        "3.4")
            local spark_full_version="3.4.3"
            local spark_tarball="spark-3.4.3-bin-hadoop3.tgz"
            local extracted_dirname="spark-3.4.3-bin-hadoop3"
            ;;
        "3.5")
            local spark_full_version="3.5.1"
            local spark_tarball="spark-3.5.1-bin-hadoop3.tgz"
            local extracted_dirname="spark-3.5.1-bin-hadoop3"
            ;;
        *)
            echo "ERROR: Unsupported Spark version: $spark_version"
            exit 1
            ;;
    esac
    
    local spark_dir="$SCRIPT_DIR/spark-versions/spark-${spark_full_version}"
    local spark_bin="$spark_dir/bin/spark-shell"
    
    # Check if Spark binary already exists
    if [ -f "$spark_bin" ]; then
        echo "Spark $spark_full_version already available at $spark_dir" >&2
        echo "$spark_dir"
        return 0
    fi
    
    echo "Downloading Spark $spark_full_version..." >&2
    
    local download_url="https://archive.apache.org/dist/spark/spark-${spark_full_version}/${spark_tarball}"
    local temp_tarball="/tmp/${spark_tarball}"
    
    # Download Spark binary
    if ! curl -L -o "$temp_tarball" "$download_url"; then
        echo "ERROR: Failed to download Spark $spark_full_version" >&2
        exit 1
    fi
    
    # Extract to spark-versions directory
    echo "Extracting Spark $spark_full_version..." >&2
    mkdir -p "$SCRIPT_DIR/spark-versions"
    
    if ! tar -xzf "$temp_tarball" -C "$SCRIPT_DIR/spark-versions/"; then
        echo "ERROR: Failed to extract Spark $spark_full_version" >&2
        rm -f "$temp_tarball"
        exit 1
    fi
    
    # Rename extracted directory to our expected name
    local extracted_dir="$SCRIPT_DIR/spark-versions/$extracted_dirname"
    if [ -d "$extracted_dir" ]; then
        mv "$extracted_dir" "$spark_dir"
    fi
    
    # Clean up tarball
    rm -f "$temp_tarball"
    
    # Add a small delay to ensure filesystem sync
    echo "Waiting for filesystem sync..." >&2
    sleep 1
    
    echo "Spark $spark_full_version ready at $spark_dir" >&2
    echo "$spark_dir"
}

# Function to generate a fixture table for a specific Hudi version
generate_fixture() {
    local hudi_version=$1
    local table_version=$2
    local fixture_name=$3
    local spark_version=$4
    local scala_version=$5
    
    echo "📊 Generating fixture: $fixture_name (Hudi $hudi_version, Table Version $table_version)"
    echo "   Using Spark $spark_version with Scala $scala_version"
    
    local fixture_path="$FIXTURES_DIR/$fixture_name"
    
    # Clean existing fixture directory to prevent table type conflicts
    if [ -d "$fixture_path" ]; then
        echo "Cleaning existing fixture directory: $fixture_path"
        rm -rf "$fixture_path"
    fi
    
    mkdir -p "$fixture_path"
    
    # Ensure Spark binary is available
    local spark_home=$(ensure_spark_binary "$spark_version")
    echo "DEBUG: spark_home=[$spark_home]"
    
    # Validate Spark installation
    if [ ! -f "$spark_home/bin/spark-shell" ]; then
        echo "ERROR: spark-shell not found at $spark_home/bin/spark-shell"
        echo "   Directory contents:"
        ls -la "$spark_home/bin/" || echo "   Directory does not exist"
        exit 1
    fi
    echo "Validated spark-shell exists at: $spark_home/bin/spark-shell"
    
    # Prepare Scala script from template
    local table_name="${fixture_name}_table"
    local temp_script="/tmp/generate_${fixture_name}.scala"
    
    # Copy template and substitute variables
    cp "$SCRIPT_DIR/scala-templates/generate-fixture.scala" "$temp_script"
    sed -i.bak \
        -e "s/\${FIXTURE_NAME}/$fixture_name/g" \
        -e "s/\${TABLE_NAME}/$table_name/g" \
        -e "s|\${BASE_PATH}|$fixture_path|g" \
        "$temp_script"
    rm -f "${temp_script}.bak"
    
    # Run Spark shell directly to generate the fixture
    echo "Running Spark shell directly from: $spark_home"
    
    # Create temporary directory for Ivy cache to avoid permission issues
    local ivy_cache_dir="/tmp/ivy-cache-${fixture_name}"
    mkdir -p "$ivy_cache_dir"
    
    # Handle version 9 specially (use local JAR instead of Maven)
    if [ "$table_version" = "9" ]; then
        echo "Using local Hudi bundle: $HUDI_BUNDLE_PATH"
        "$spark_home/bin/spark-shell" \
            --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
            --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
            --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
            --conf "spark.jars.ivy=$ivy_cache_dir" \
            --conf 'spark.sql.warehouse.dir=/tmp/spark-warehouse' \
            --jars "$HUDI_BUNDLE_PATH" -i "$temp_script"
    else
        # Use Maven packages for official releases
        local hudi_bundle="org.apache.hudi:hudi-spark${spark_version}-bundle_${scala_version}:${hudi_version}"
        echo "Using Hudi bundle: $hudi_bundle"
        "$spark_home/bin/spark-shell" \
            --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
            --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
            --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
            --conf "spark.jars.ivy=$ivy_cache_dir" \
            --conf 'spark.sql.warehouse.dir=/tmp/spark-warehouse' \
            --packages "$hudi_bundle" -i "$temp_script"
    fi
    
    # Clean up ivy cache directory
    rm -rf "$ivy_cache_dir"
    
    # Clean up temp script
    rm "/tmp/generate_${fixture_name}.scala"
    
    echo "Fixture $fixture_name generated successfully"
}

# Generate fixtures for each version using appropriate Spark versions
echo "Generating fixtures for all supported versions..."

# Based on Hudi-Spark compatibility matrix:
# 1.0.x    -> 3.5.x (default)
# 0.15.x   -> 3.5.x (default)
# 0.14.x   -> 3.4.x (default)
# 0.13.x   -> 3.3.x (default)
# 0.12.x   -> 3.3.x (default)
# 0.11.x   -> 3.2.x (default)

echo "Hudi Version -> Spark Version -> Scala Version mapping:"

# Hudi 0.11.1 (Table Version 4) -> Spark 3.2.x (default) -> Scala 2.12
if should_generate_version "4"; then
    echo "   0.11.1 -> Spark 3.2 -> Scala 2.12"
    generate_fixture "0.11.1" "4" "hudi-v4-table" "3.2" "2.12"
fi

# Hudi 0.12.2 (Table Version 5) -> Spark 3.3.x (default) -> Scala 2.12  
if should_generate_version "5"; then
    echo "   0.12.2 -> Spark 3.3 -> Scala 2.12"
    generate_fixture "0.12.2" "5" "hudi-v5-table" "3.3" "2.12"
fi

# Hudi 0.14.0 (Table Version 6) -> Spark 3.4.x (default) -> Scala 2.12
if should_generate_version "6"; then
    echo "   0.14.0 -> Spark 3.4 -> Scala 2.12"
    generate_fixture "0.14.0" "6" "hudi-v6-table" "3.4" "2.12"
fi

# Hudi 1.0.2 (Table Version 8) -> Spark 3.5.x (default) -> Scala 2.12
if should_generate_version "8"; then
    echo "   1.0.2 -> Spark 3.5 -> Scala 2.12"
    generate_fixture "1.0.2" "8" "hudi-v8-table" "3.5" "2.12"
fi

# Hudi 1.1.0 (Table Version 9) -> Spark 3.5.x (default) -> Scala 2.12 (requires local bundle)
if should_generate_version "9"; then
    echo "   1.1.0 -> Spark 3.5 -> Scala 2.12 (using local bundle)"
    generate_fixture "1.1.0" "9" "hudi-v9-table" "3.5" "2.12"
fi


echo ""
echo "Fixture generation completed!"

# Compress fixture tables to save space
echo ""
echo "Compressing fixture tables..."
for fixture_dir in "$FIXTURES_DIR"/hudi-v*-table; do
    if [ -d "$fixture_dir" ]; then
        fixture_name=$(basename "$fixture_dir")
        echo "Compressing $fixture_name..."
        (cd "$FIXTURES_DIR" && zip -r -q -X "${fixture_name}.zip" "$fixture_name")
        if [ $? -eq 0 ]; then
            rm -rf "$fixture_dir"
            echo "Created ${fixture_name}.zip"
        else
            echo "ERROR: Failed to compress $fixture_name"
            exit 1
        fi
    fi
done

echo ""
echo "Compression completed!"
echo "Generated compressed fixtures:"
find "$FIXTURES_DIR" -name "hudi-v*-table.zip" | sort
