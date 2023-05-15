import fsspec
import hoodieInstant
from hoodieActiveTimeline import VALID_EXTENSIONS_IN_ACTIVE_TIMELINE


class HoodieTableMetaClient:
    def  __init__(self, fs: fsspec.AbstractFileSystem, basePath):
        self.fs = fs
        self.basePath = basePath

    def scanHoodieInstantsFromFileSystem(self):
        files = self.fs.ls(self.basePath + "/.hoodie")
        instants = []
        for file in files:
            extension = hoodieInstant.HoodieInstant.getTimelineFileExtension(file)
            if extension is not None and extension in VALID_EXTENSIONS_IN_ACTIVE_TIMELINE:
                instants.append(file)
        return instants
    

