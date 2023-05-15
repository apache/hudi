import re


class HoodieInstant:
    
    def getTimelineFileExtension(filename: str):
        stripName = filename.rsplit("/")[-1]
        matches = re.findall("(\d+)(\.\w+)(\.\D+)?", stripName)
        if len(matches) > 0:
            return stripName[len(matches[0][0]):]
    
