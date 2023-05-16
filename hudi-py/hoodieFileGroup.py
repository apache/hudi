from hoodieBaseFile import HoodieBaseFile


class HoodieFileGroup:
    def __init__(self, fileGroupId: str, partition: str):
        self.fileGroupId = fileGroupId
        self.lastInstant = None
        self.partition = partition
        self.fileSlices = {}

    def addBaseFile(self, dataFile: HoodieBaseFile):
        if not dataFile.commitTime in self.fileSlices:
            self.fileSlices[dataFile.commitTime] = dataFile
        if self.lastInstant is None or self.lastInstant < dataFile.commitTime:
            self.lastInstant = dataFile.commitTime

    def getLatestFileSliceBefore(self, maxInstantTime: int):
        if self.lastInstant is None or maxInstantTime > self.lastInstant:
            return None
        return self.fileSlices[max(k for k in self.fileSlices if k <= maxInstantTime)]
    
    def getLatestFileSlice(self):
        if self.lastInstant is None:
            return None
        return self.fileSlices[self.lastInstant]
        
