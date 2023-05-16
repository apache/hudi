import fsspec
from hoodieInstant import HoodieInstant
from hoodieActiveTimeline import VALID_EXTENSIONS_IN_ACTIVE_TIMELINE


class HoodieTableMetaClient:
    def  __init__(self, fs: fsspec.AbstractFileSystem, basePath):
        self.fs = fs
        self.basePath = basePath

    def scanHoodieInstantsFromFileSystem(self):
        files = self.fs.ls(self.basePath + "/.hoodie")
        instants = []
        for file in files:
            extension = HoodieInstant.getTimelineFileExtension(file)
            if extension is not None and extension in VALID_EXTENSIONS_IN_ACTIVE_TIMELINE:
                instants.append(file)
        return instants
    
    #{'name': '/tmp/hudi_trips_cow/americas', 'size': 128, 'type': 'directory', 'created': 1683997424.349733, 'islink': False, 'mode': 16877, 'uid': 501, 'gid': 0, 'mtime': 1683997424.349733, 'ino': 67754073, 'nlink': 4}
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

    

