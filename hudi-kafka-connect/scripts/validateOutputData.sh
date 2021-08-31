# Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/bash

python fetchAllCommitedLogs.py | jq '.volume' | sort -n | awk 'BEGIN{exp_number=0;duplicates=0;missing_records=0;}{if ($1>exp_number) { print $1, " -- ", exp_number; missing_records=missing_records+($1-exp_number)+1; exp_number=$1+1;} if($1<exp_number) {duplicates = duplicates + 1} if ($1 == exp_number) {exp_number = exp_number + 1;} total_records=total_records+1;}END{print "Duplicate records=", duplicates, "Missing Records=", missing_records, "Total Records=", total_records}'

