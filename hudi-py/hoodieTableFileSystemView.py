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

from hoodieFileGroup import HoodieFileGroup
from hoodieTableMetaClient import HoodieTableMetaClient
from hoodieBaseFile import HoodieBaseFile


BASE_FILE_EXTENSIONS = [".parquet", ".orc"]

def getInvalidFilesForPartition(invalidFGS: dict[str, set[str]], partition: str) -> set[str]:
    if partition in invalidFGS:
        return invalidFGS[partition]
    return set()

class HoodieTableFileSystemView:
    def __init__(self, metaClient: HoodieTableMetaClient):
        self.metaClient = metaClient
        self.partitionToFileGroupsMap: dict[str, dict[str, HoodieFileGroup]] = {}

    def _ensurePartitionLoadedCorrectly(self, partition: str):
        if partition not in self.partitionToFileGroupsMap:
            partitionPath = self.metaClient.basePath + "/" + partition
            files = self.metaClient.fs.ls(partitionPath)
            self.partitionToFileGroupsMap[partition] = self._addFilesToView(files, partition)

    def _ensurePartitionsLoadedCorrectly(self):
        partitions = self.metaClient.getPartitions()
        for partition in partitions:
            self._ensurePartitionLoadedCorrectly(partition)
    

    def _getLatestFilesInPartitionAsOf(self, partition: str, maxInstantTime: int, invalidFilegroups: set[str]) -> list[str]:
        fileIdtoFileGroup = self.partitionToFileGroupsMap[partition]
        baseFiles = [fileIdtoFileGroup[fileId].getLatestFileSliceAsOf(maxInstantTime) for fileId in fileIdtoFileGroup]
        return [baseFile.filePath for baseFile in baseFiles if not baseFile is None and not baseFile.fileId in invalidFilegroups]
    
    def getLatestFiles(self) -> list[str]:
        latestTimestamp = self.metaClient.getActiveTimeline().getCompletedInstants().last().timestamp
        return self.getLatestFilesAsOf(latestTimestamp)
        
    
    def getLatestFilesAsOf(self, maxInstantTime: int):
        self._ensurePartitionsLoadedCorrectly()
        invalidFGS = self.metaClient.getInvalidFilegroupsAsOf(maxInstantTime)
        return [file for partition in self.partitionToFileGroupsMap 
                for file in self._getLatestFilesInPartitionAsOf(partition, maxInstantTime, getInvalidFilesForPartition(invalidFGS, partition))]
    

    def _addFilesToView(self, files: list[str], partition: str) -> dict[str, HoodieFileGroup]:
        return self._buildFileGroups(self._convertFilesToBaseFiles(files), partition)

    def _isBaseFilePath(self, fileName):
        for extension in BASE_FILE_EXTENSIONS:
            if fileName.endswith(extension):
                return True
        return False
    
    def getPartitions(self): 
        self.metaClient.getPartitions()

    def _convertFilesToBaseFiles(self, files: list[str]) -> list[HoodieBaseFile]:
        return [HoodieBaseFile(file) for file in files if self._isBaseFilePath(file)]

    def _buildFileGroups(self, basefiles: list[HoodieBaseFile], partition: str) -> dict[str, HoodieFileGroup]:
        fileIdtoFileGroup: dict[str, HoodieFileGroup] = {}
        for file in basefiles:
            if not file.fileId in fileIdtoFileGroup:
                fileIdtoFileGroup[file.fileId] = HoodieFileGroup(file.fileId, partition)
            fileIdtoFileGroup[file.fileId].addBaseFile(file)
        return fileIdtoFileGroup


