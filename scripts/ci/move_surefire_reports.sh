#!/bin/bash

# Check if two arguments were provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_directory> <destination_directory>"
    exit 1
fi

# Assign the first and second argument to SOURCE and DEST variables
SOURCE="$1"
DEST="$2"

# Ensure the source directory exists
if [ ! -d "$SOURCE" ]; then
    echo "Source directory does not exist: $SOURCE"
    exit 1
fi

# Create the destination directory if it doesn't exist
if [ ! -d "$DEST" ]; then
    mkdir -p "$DEST"
fi

find "$SOURCE" -type f -name "TEST-*.xml" | while IFS= read -r file; do
    # Extract the relative directory path
    relative_path="${file#$SOURCE}"
    destination_path="$DEST$relative_path"
    destination_dir=$(dirname "$destination_path")

    # Create the destination directory if it doesn't exist
    mkdir -p "$destination_dir"

    # Move the file to the new location, preserving the directory structure
    mv "$file" "$destination_path"
done
