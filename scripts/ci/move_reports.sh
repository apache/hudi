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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# This script is aimed to fix the file access permission issue in Docker container:
##[error]Error: Failed find: EACCES: permission denied, scandir '/home/vsts/work/1/s/hudi-utilities/null/hive'

# Check if two arguments were provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_directory> <destination_directory>"
    exit 1
fi

# Assign the first and second argument to SOURCE and DEST variables
SOURCE="$1"
DEST="$2"
# List of file patterns
LIST=( -name "TEST-*.xml" -o -name "jacoco.xml" )

# Ensure the source directory exists
if [ ! -d "$SOURCE" ]; then
    echo "Source directory does not exist: $SOURCE"
    exit 1
fi

# Create the destination directory if it doesn't exist
if [ ! -d "$DEST" ]; then
    mkdir -p "$DEST"
fi

find -L "$SOURCE" -type f \( "${LIST[@]}" \)  | while IFS= read -r file; do
    # Extract the relative directory path
    relative_path="${file#$SOURCE}"
    destination_path="$DEST$relative_path"
    destination_dir=$(dirname "$destination_path")

    if [[ "$relative_path" == *"scripts/ci"* ]]; then
        continue # Skip this file
    fi

    # Create the destination directory if it doesn't exist
    mkdir -p "$destination_dir"

    # Move the file to the new location, preserving the directory structure
    mv "$file" "$destination_path"
done

# Print the file copies
echo 'All surefire report files:'
find "$DEST" -type f -name "TEST-*.xml"

echo 'All jacoco report files:'
find "$DEST" -type f -name "jacoco.xml"
