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

echo "Generating Hudi upgrade/downgrade test fixtures..."

# Parse command line arguments
REQUESTED_VERSIONS=""
HUDI_BUNDLE_PATH=""
SCALA_SCRIPT_NAME="generate-fixture-mor.scala"
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
        --script-name)
            SCALA_SCRIPT_NAME="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--version <version_list>] [--hudi-bundle-path <path>] [--script-name <script>]"
            echo "  --version <version_list>          Comma-separated list of table versions to generate (e.g., 4,5,6)"
            echo "  --hudi-bundle-path <path>         Path to locally built Hudi bundle JAR (required for version 9)"
            echo "  --script-name <script>            Scala script name from scala-templates folder (default: generate-fixture-mor.scala)"
            echo ""
            echo "Examples:"
            echo "  $0                                           # Generate all versions (except 9)"
            echo "  $0 --version 4,5                             # Generate versions 4 and 5"
            echo "  $0 --script-name generate-fixture-complex-keygen.scala  # Use complex keygen script"
            echo "  $0 --version 9 --hudi-bundle-path /path/to/hudi-spark3.5-bundle_2.12-1.1.0-SNAPSHOT.jar"
            echo ""
            echo "Note: Version 9 requires a locally built Hudi bundle from master branch"
            exit 1
            ;;
    esac
done

# Determine zip suffix based on script name
# Extract suffix by removing "generate-fixture" prefix and ".scala" extension
SCRIPT_BASE="${SCALA_SCRIPT_NAME%.scala}"  # Remove .scala extension
SCRIPT_SUFFIX="${SCRIPT_BASE#generate-fixture}"  # Remove generate-fixture prefix

# Determine output directory based on template type
if [[ "$SCALA_SCRIPT_NAME" == *"mor"* ]]; then
    FIXTURES_DIR="$SCRIPT_DIR/mor-tables"
    echo "Using mor tables directory: $FIXTURES_DIR"
elif [[ "$SCALA_SCRIPT_NAME" == *"cow"* ]]; then
    FIXTURES_DIR="$SCRIPT_DIR/cow-tables"
    echo "Using cow tables directory: $FIXTURES_DIR"
elif [[ "$SCALA_SCRIPT_NAME" == *"complex-keygen"* ]]; then
    FIXTURES_DIR="$SCRIPT_DIR/complex-keygen-tables"
    echo "Using complex-keygen tables directory: $FIXTURES_DIR"
elif [[ "$SCALA_SCRIPT_NAME" == *"payload"* ]]; then
    FIXTURES_DIR="$SCRIPT_DIR/payload-tables"
    echo "Using payload tables directory: $FIXTURES_DIR"
else
    # Default fallback
    FIXTURES_DIR="$SCRIPT_DIR/complex-keygen-tables"
    echo "Using default tables directory: $FIXTURES_DIR"
fi

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
    export SPARK_HOME=$(ensure_spark_binary "$spark_version")
    echo "DEBUG: spark_home=[$spark_home]"
    echo "SPARK_HOME=$SPARK_HOME"

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

    # Adjust script name for version 9
    local actual_script_name="$SCALA_SCRIPT_NAME"
    if [ "$table_version" = "9" ]; then
        # Add -v9 before .scala extension
        actual_script_name="${SCALA_SCRIPT_NAME%.scala}-v9.scala"
    fi

    # Validate template file exists
    if [ ! -f "$SCRIPT_DIR/scala-templates/$actual_script_name" ]; then
        echo "ERROR: Template file not found: $SCRIPT_DIR/scala-templates/$actual_script_name"
        echo "Available templates:"
        ls "$SCRIPT_DIR/scala-templates/"*.scala 2>/dev/null || echo "  No templates found"
        exit 1
    fi

    # Copy template and substitute variables
    cp "$SCRIPT_DIR/scala-templates/$actual_script_name" "$temp_script"
    # For payload tables, use the fixtures directory as base path, not the specific fixture path
    if [[ "$SCALA_SCRIPT_NAME" == *"payload"* ]]; then
        sed -i.bak \
            -e "s/\${FIXTURE_NAME}/$fixture_name/g" \
            -e "s/\${TABLE_NAME}/$table_name/g" \
            -e "s|\${BASE_PATH}|$FIXTURES_DIR|g" \
            "$temp_script"
    else
        sed -i.bak \
            -e "s/\${FIXTURE_NAME}/$fixture_name/g" \
            -e "s/\${TABLE_NAME}/$table_name/g" \
            -e "s|\${BASE_PATH}|$fixture_path|g" \
            "$temp_script"
    fi
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
            --driver-memory "2g" \
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
            --driver-memory "2g" \
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


# Create fixtures directory
mkdir -p "$FIXTURES_DIR"

# Hudi 0.14.0 (Table Version 6) -> Spark 3.4.x (default) -> Scala 2.12
if should_generate_version "6"; then
    echo "   0.14.0 -> Spark 3.4 -> Scala 2.12"
    generate_fixture "0.14.0" "6" "hudi-v6-table$SCRIPT_SUFFIX" "3.4" "2.12"
fi

# Hudi 1.0.2 (Table Version 8) -> Spark 3.5.x (default) -> Scala 2.12
if should_generate_version "8"; then
    echo "   1.0.2 -> Spark 3.5 -> Scala 2.12"
    generate_fixture "1.0.2" "8" "hudi-v8-table$SCRIPT_SUFFIX" "3.5" "2.12"
fi

# Hudi 1.1.0 (Table Version 9) -> Spark 3.5.x (default) -> Scala 2.12 (requires local bundle)
if should_generate_version "9"; then
    echo "   1.1.0 -> Spark 3.5 -> Scala 2.12 (using local bundle)"
    generate_fixture "1.1.0" "9" "hudi-v9-table$SCRIPT_SUFFIX" "3.5" "2.12"
fi


echo ""
echo "Fixture generation completed!"

# Compress fixture tables to save space
echo ""
echo "Compressing fixture tables..."

# Handle payload tables differently (multiple tables created by one script)
if [[ "$SCALA_SCRIPT_NAME" == *"payload"* ]]; then
    # For payload tables, compress each individual payload table directory
    for fixture_dir in "$FIXTURES_DIR"/hudi-v*-table-payload-*; do
        if [ -d "$fixture_dir" ]; then
            fixture_name=$(basename "$fixture_dir")
            echo "Compressing $fixture_name..."
            zip_name="${fixture_name}.zip"
            (cd "$FIXTURES_DIR" && zip -r -q -X "$zip_name" "$fixture_name")
            if [ $? -eq 0 ]; then
                rm -rf "$fixture_dir"
                echo "Created $zip_name"
            else
                echo "ERROR: Failed to compress $fixture_name"
                exit 1
            fi
        fi
    done
else
    # Handle regular tables (one table per script)
    for fixture_dir in "$FIXTURES_DIR"/hudi-v*-table"$SCRIPT_SUFFIX"; do
        if [ -d "$fixture_dir" ]; then
            fixture_name=$(basename "$fixture_dir")
            echo "Compressing $fixture_name..."
            zip_name="${fixture_name}.zip"
            (cd "$FIXTURES_DIR" && zip -r -q -X "$zip_name" "$fixture_name")
            if [ $? -eq 0 ]; then
                rm -rf "$fixture_dir"
                echo "Created $zip_name"
            else
                echo "ERROR: Failed to compress $fixture_name"
                exit 1
            fi
        fi
    done
fi

echo ""
echo "Compression completed!"
echo "Generated compressed fixtures:"
find "$FIXTURES_DIR" -name "hudi-v*-table*.zip" | sort
