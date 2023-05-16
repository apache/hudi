from hoodieFileGroup import HoodieFileGroup
from hoodieTableMetaClient import HoodieTableMetaClient
from hoodieBaseFile import HoodieBaseFile


BASE_FILE_EXTENSIONS = [".parquet", ".orc"]

class HoodieTableFileSystemView:
    def __init__(self, metaClient: HoodieTableMetaClient):
        self.metaClient = metaClient
        self.partitionToFileGroupsMap = {}
        
    def _ensurePartitionLoadedCorrectly(self, partition: str):
        if partition not in self.partitionToFileGroupsMap:
            partitionPath = self.metaClient.basePath + "/" + partition
            files = self.metaClient.fs.ls(partitionPath)
            self.partitionToFileGroupsMap[partition] = self._addFilesToView(files, partition)

    def _ensurePartitionsLoadedCorrectly(self):
        partitions = self.metaClient.getPartitions()
        for partition in partitions:
            self._ensurePartitionLoadedCorrectly(partition)
    
    def _getLatestFilesInPartition(self, partition: str):
        fileIdtoFileGroup = self.partitionToFileGroupsMap[partition]
        fileSlices = [fileIdtoFileGroup[fileId].getLatestFileSlice() for fileId in fileIdtoFileGroup]
        return [fileSlice.filePath for fileSlice in fileSlices if not fileSlice is None]
    
    def _getLatestFilesInPartitionBefore(self, partition: str, maxInstantTime: int):
        fileIdtoFileGroup = self.partitionToFileGroupsMap[partition]
        fileSlices = [fileIdtoFileGroup[fileId].getLatestFileSliceBefore(maxInstantTime) for fileId in fileIdtoFileGroup]
        return [fileSlice.filePath for fileSlice in fileSlices if not fileSlice is None]
    
    def getLatestFiles(self):
        self._ensurePartitionsLoadedCorrectly()
        return [file for partition in self.partitionToFileGroupsMap for file in self._getLatestFilesInPartition(partition)]
    
    def getLatestFilesBefore(self, maxInstantTime: int):
        self._ensurePartitionsLoadedCorrectly()
        return [file for partition in self.partitionToFileGroupsMap for file in self._getLatestFilesInPartitionBefore(partition, maxInstantTime)]

    def _addFilesToView(self, files: list[str], partition: str):
        return self._buildFileGroups(self._convertFilesToBaseFiles(files), partition)

    def _isBaseFilePath(self, fileName):
        for extension in BASE_FILE_EXTENSIONS:
            if fileName.endswith(extension):
                return True
        return False
    
    def getPartitions(self): 
        self.metaClient.getPartitions()

    def _convertFilesToBaseFiles(self, files: list[str]):
        return [HoodieBaseFile(file) for file in files if self._isBaseFilePath(file)]

    def _buildFileGroups(self, basefiles: list[HoodieBaseFile], partition: str):
        fileIdtoFileGroup = {}
        for file in basefiles:
            if not file.fileId in fileIdtoFileGroup:
                fileIdtoFileGroup[file.fileId] = HoodieFileGroup(file.fileId, partition)
            fileIdtoFileGroup[file.fileId].addBaseFile(file)
        return fileIdtoFileGroup


