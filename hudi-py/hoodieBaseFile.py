import os


class HoodieBaseFile:
    def __init__(self, filePath: str):
        self.filePath = filePath
        self.fileName = os.path.basename(filePath)
        self.fileId = self.fileName.split("_")[0]
        self.commitTime = int(self.fileName.split("_")[2].split(".")[0])


