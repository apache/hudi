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

#!/bin/python

import os

# Fetch all commited log times
commitFiles = []
for file in os.listdir("/tmp/"):
	if file.startswith(".hudi"):
		fileName = file.split("-", 2)
		commitFiles.append(fileName[1])

#print("Number of commited logs ", len(commitFiles))

writtenLogs = []
for file in os.listdir("/tmp/"):
	if file.startswith("hudi-test-topic"):
		writtenLogs.append("/tmp/" + file)

#print("Number of Total written logs ", len(writtenLogs))

commitedLogs = []
for writtenLog in writtenLogs:
	for commit in commitFiles:
		if (writtenLog.find(commit) >= 0):
			commitedLogs.append(writtenLog)


#print("Number of Total written logs that were commited ", len(commitedLogs))

for commitedLog in commitedLogs:
	commitedLogFile = open(commitedLog)
	lines = commitedLogFile. readlines()
	for line in lines:
		print(line)
	commitedLogFile.close()
