#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from hoodieBaseFile import HoodieBaseFile


class HoodieFileGroup:
    def __init__(self, fileGroupId: str, partition: str):
        self.fileGroupId = fileGroupId
        self.lastInstant = None
        self.firstInstant = None
        self.partition = partition
        self.fileSlices: dict[str, HoodieBaseFile] = {}

    def addBaseFile(self, dataFile: HoodieBaseFile):
        if not dataFile.commitTime in self.fileSlices:
            self.fileSlices[dataFile.commitTime] = dataFile
        if self.lastInstant is None or self.lastInstant < dataFile.commitTime:
            self.lastInstant = dataFile.commitTime
        if self.firstInstant is None or self.firstInstant > dataFile.commitTime:
            self.firstInstant = dataFile.commitTime

    def getLatestFileSliceAsOf(self, maxInstantTime: str) -> (HoodieBaseFile | None):
        if self.lastInstant is None or maxInstantTime < self.firstInstant:
            return None
        return self.fileSlices[max(k for k in self.fileSlices if k <= maxInstantTime)]
    
        
