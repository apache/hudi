#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from hoodieTableFileSystemView import HoodieTableFileSystemView
import hoodieTableMetaClient
from fsspec.implementations.local import LocalFileSystem
import pyarrow.parquet as pq
import pandas as pd


class Hudi:
    def __init__(self, basePath: str):
        self._metaClient = hoodieTableMetaClient.HoodieTableMetaClient(LocalFileSystem(), basePath)
        self._fs = HoodieTableFileSystemView(self._metaClient)

    def read_table(self):
        return pq.read_table(self._fs.getLatestFiles())
    
    def read_table_as_of(self, maxInstantTime: int):
        return pq.read_table(self._fs.getLatestFilesAsOf(maxInstantTime))


def main():
    hudi = Hudi("/tmp/hudi_trips_cow")
    arrowtable = hudi.read_table()
    pd.set_option('display.max_columns', None)
    pandasDf =  arrowtable.to_pandas()
    print(pandasDf.sort_values("fare")[["fare","_hoodie_record_key", "_hoodie_commit_time"]].head(10))

if __name__ == "__main__":
    main()