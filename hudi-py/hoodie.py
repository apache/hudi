from hoodieTableFileSystemView import HoodieTableFileSystemView
import hoodieTableMetaClient
from fsspec.implementations.local import LocalFileSystem







def main():
    metaClient = hoodieTableMetaClient.HoodieTableMetaClient(LocalFileSystem(), "/tmp/hudi_trips_cow")
    files = metaClient.scanHoodieInstantsFromFileSystem()
    fs = HoodieTableFileSystemView(metaClient)
    fs.addPartition("americas/united_states/san_francisco")
    fs.addPartition("asia/india/chennai")
    fs.addPartition("americas/brazil/sao_paulo")
    # for partition in fs.partitionToFileGroupsMap:
    #     print("partition is " + partition)
    #     print(fs.partitionToFileGroupsMap[partition])


if __name__ == "__main__":
    main()