#!/bin/bash
# This script assumes conventional Maven directory structure and is expected to be run from the project root folder.

SRC_MAIN_JAVA='src/main/java'

find . -path "**/$SRC_MAIN_JAVA" -prune | while read dot_basedir_src_main_java; do

#   basedir_src_main_java="${dot_basedir_src_main_java#./}" # remove leading "./"
    basedir="${dot_basedir_src_main_java%/$SRC_MAIN_JAVA}" # remove trailing "/src/main/java"
    jacocoxml="$basedir/target/site/jacoco-ut/jacoco.xml"
    if [ ! -f "$jacocoxml" ]; then
        jacocoxml="$basedir/build/reports/jacoco.xml"
    fi
    cobertura="$basedir/target/site/cobertura"

    printf "Converting JaCoCo report for $dot_basedir_src_main_java based on $jacocoxml... "
    if [ -f "$jacocoxml" ]; then
      mkdir -p "$cobertura"
      .jenkins/cover2cover.py "$jacocoxml" "$basedir/$SRC_MAIN_JAVA" > "$cobertura/coverage.xml" \
        && echo "Done." \
        || echo "Failed!"
    else
      echo "No test coverage found!"
    fi

done

echo "Took $SECONDS sec to execute $0 $@"

