import hoodieTableMetaClient
from fsspec.implementations.local import LocalFileSystem







def main():
    metaClient = hoodieTableMetaClient.HoodieTableMetaClient(LocalFileSystem(), "/tmp/hudi_trips_cow")
    files = metaClient.scanHoodieInstantsFromFileSystem()
    print(files)


if __name__ == "__main__":
    main()