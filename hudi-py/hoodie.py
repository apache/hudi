from hoodieTableFileSystemView import HoodieTableFileSystemView
import hoodieTableMetaClient
from fsspec.implementations.local import LocalFileSystem
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


class Hoodie:
    def __init__(self, basePath: str):
        self._metaClient = hoodieTableMetaClient.HoodieTableMetaClient(LocalFileSystem(), basePath)
        self._fs = HoodieTableFileSystemView(self._metaClient)


    def read_table(self):
        return pq.read_table(self._fs.getLatestFiles())
    
    def read_table_before(self, maxInstantTime: int):
        return pq.read_table(self._fs.getLatestFilesBefore(maxInstantTime))





def main():
    hudi = Hoodie("/tmp/hudi_trips_cow")
    arrowtable = hudi.read_table()
    pd.set_option('display.max_columns', None)
    pandasDf =  arrowtable.to_pandas()
    pandasasofDf = hudi.read_table_before(20230517192402200).to_pandas()
    print(pandasDf)
    print(pandasasofDf)

if __name__ == "__main__":
    main()