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

### Checking for DISCLAIMER
echo "Checking for DISCLAIMER"
disclaimerFile="./DISCLAIMER"
if [ -f "$disclaimerFile" ]; then
  echo "DISCLAIMER file should not be present [ERROR]"
  exit 1
fi
echo -e "\t\tDISCLAIMER file exists ? [OK]\n"

### Checking for LICENSE and NOTICE
echo "Checking for LICENSE and NOTICE"
licenseFile="./LICENSE"
noticeFile="./NOTICE"
if [ ! -f "$licenseFile" ]; then
  echo "License file missing [ERROR]"
  exit 1
fi
echo -e "\t\tLicense file exists ? [OK]"

if [ ! -f "$noticeFile" ]; then
  echo "Notice file missing [ERROR]"
  exit 1
fi
echo -e "\t\tNotice file exists ? [OK]\n"

### Licensing Check
echo "Performing custom Licensing Check "
# ---
# Exclude the 'hudi-trino-plugin' directory. Its license checks are handled by airlift:
# https://github.com/airlift/airbase/blob/823101482dbc60600d7862f0f5c93aded6190996/airbase/pom.xml#L1239
# ---
numfilesWithNoLicense=$(find . -path './hudi-trino-plugin' -prune -o -type f -iname '*' | grep -v './hudi-trino-plugin' | grep -v NOTICE | grep -v LICENSE | grep -v '.jpg' | grep -v '.json' | grep -v '.zip' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v emptyFile | grep -v DISCLAIMER | grep -v '.sqltemplate' | grep -v KEYS | grep -v '.mailmap' | grep -v 'banner.txt' | grep -v '.txt' | grep -v "fixtures" | xargs grep -L "Licensed to the Apache Software Foundation (ASF)")
# Check if the variable holding the list of files is non-empty
if [ -n "$numfilesWithNoLicense" ]; then
  # If the list isn't empty, count the files and report the error
  numFiles=$(echo "$numfilesWithNoLicense" | wc -l)
  echo "There were ${numFiles} source files that did not have Apache License [ERROR]"
  echo "$numfilesWithNoLicense"
  exit 1
fi
echo -e "\t\tLicensing Check Passed [OK]\n"
