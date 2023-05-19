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

import fsspec
from hoodieInstant import HoodieInstant
from hoodieInstant import VALID_EXTENSIONS_IN_ACTIVE_TIMELINE
from hoodieTimeline import HoodieTimeline


class HoodieTableMetaClient:
    def  __init__(self, fs: fsspec.AbstractFileSystem, basePath):
        self.fs = fs
        self.basePath = basePath
        self._activeTimeline = self._initActiveTimeline()

    def _scanHoodieInstantsFromFileSystem(self):
        files = self.fs.ls(self.basePath + "/.hoodie")
        instants = []
        for file in files:
            instant = HoodieInstant(file, self.fs)
            if instant.valid  and instant.extension in VALID_EXTENSIONS_IN_ACTIVE_TIMELINE:
                instants.append(instant)
        return instants
    
    def _initActiveTimeline(self):
        instants = self._scanHoodieInstantsFromFileSystem()
        list.sort(instants, key=HoodieInstant.sort_key)
        return HoodieTimeline(instants)
    

    def getActiveTimeline(self):
        return self._activeTimeline
    
    def getInvalidFilegroupsAsOf(self, timestamp: str) -> dict[str, set[str]]:
        invalidFilegroups: dict[str, set[str]] = {}
        def filterFunc(instant: HoodieInstant):
            return instant.isAsOf(timestamp) and instant.isCompleted() and instant.isReplaceCommit() 
        for replaceCommit in self.getActiveTimeline().filter(filterFunc).getInstants():
            for partition in replaceCommit.invalidFiles:
                if not partition in invalidFilegroups:
                    invalidFilegroups[partition] = replaceCommit.invalidFiles[partition]
                else:
                    invalidFilegroups[partition].union(replaceCommit.invalidFiles[partition])
        return invalidFilegroups
                    
    
    def getPartitions(self):
        pathsToList = self.fs.ls(self.basePath, True)
        return [partitionPath for path in pathsToList if path["type"] == "directory" and not path["name"].endswith(".hoodie") for partitionPath in self._getRestOfPath(path)]
        

    def _getRestOfPath(self, path):
        subPaths = self.fs.ls(path["name"], True)
        partition_path = []
        for subPath in subPaths:
            if subPath["type"] == "directory":
                partition_path = partition_path + self._getRestOfPath(subPath)
        if len(partition_path) == 0:
            partition_path.append(path["name"][len(self.basePath)+1:])
        return partition_path

    

