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
numfilesWithNoLicense=`find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.jpg' | grep -v '.json' | grep -v '.parquet' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v emptyFile | grep -v DISCLAIMER | grep -v KEYS | grep -v '.mailmap' | grep -v '.sqltemplate' | grep -v 'banner.txt' | grep -v '.txt' | grep -v "fixtures" | xargs grep -L -E "(Licensed to the Apache Software Foundation \(ASF\)|Licensed under the Apache License, Version 2\.0)" | wc -l`
if [ "$numfilesWithNoLicense" -gt  "0" ]; then
  echo "There were some source files that did not have Apache License [ERROR]"
  find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.jpg' | grep -v '.json' | grep -v '.parquet' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v emptyFile | grep -v DISCLAIMER | grep -v '.sqltemplate' | grep -v KEYS | grep -v '.mailmap' | grep -v 'banner.txt' | grep -v '.txt' | grep -v "fixtures" | xargs grep -L -E "(Licensed to the Apache Software Foundation \(ASF\)|Licensed under the Apache License, Version 2\.0)"
  exit 1
fi
echo -e "\t\tLicensing Check Passed [OK]\n"
