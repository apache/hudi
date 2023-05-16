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

    def addPartition(self, partition: str):
        self._ensurePartitionLoadedCorrectly(partition)

    def _addFilesToView(self, files: list[str], partition: str):
        return self._buildFileGroups(self._convertFilesToBaseFiles(files), partition)

    def _isBaseFilePath(self, fileName):
        for extension in BASE_FILE_EXTENSIONS:
            if fileName.endswith(extension):
                return True
        return False

    def _convertFilesToBaseFiles(self, files: list[str]):
        return [HoodieBaseFile(file) for file in files if self._isBaseFilePath(file)]

    def _buildFileGroups(self, basefiles: list[HoodieBaseFile], partition: str):
        fileIdtoFileGroup = {}
        for file in basefiles:
            if not file.fileId in fileIdtoFileGroup:
                fileIdtoFileGroup[file.fileId] = HoodieFileGroup(file.fileId, partition)
            fileIdtoFileGroup[file.fileId].addBaseFile(file)
        return fileIdtoFileGroup


