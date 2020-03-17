/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.bloom.BloomIndexFileInfo;
import org.apache.hudi.table.HoodieTable;

/**
 * Extract range & bloomFilter information for a given file slice.
 */
public class HoodieBloomRangeInfoHandle<T extends HoodieRecordPayload> extends HoodieReadHandle<T> {

  private BloomIndexFileInfo rangeInfo;
  private BloomFilter bloomFilter;

  public HoodieBloomRangeInfoHandle(HoodieWriteConfig config, HoodieTable<T> hoodieTable,
      Pair<String, String> partitionPathFilePair) {
    super(config, null, hoodieTable, partitionPathFilePair);

    Path dataFilePath = new Path(getLatestDataFile().getPath());
    this.bloomFilter = ParquetUtils.readBloomFilterFromParquetMetadata(hoodieTable.getHadoopConf(), dataFilePath);

    try {
      String[] minMaxKeys = ParquetUtils.readMinMaxRecordKeys(hoodieTable.getHadoopConf(), dataFilePath);
      this.rangeInfo = new BloomIndexFileInfo(partitionPathFilePair.getRight(), minMaxKeys[0], minMaxKeys[1]);
    } catch (MetadataNotFoundException me) {
      this.rangeInfo = new BloomIndexFileInfo(partitionPathFilePair.getRight());
    }
  }

  public BloomIndexFileInfo getRangeInfo() {
    return rangeInfo;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }
}
