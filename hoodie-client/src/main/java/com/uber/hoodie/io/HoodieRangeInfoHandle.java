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

package com.uber.hoodie.io;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.fs.Path;

/**
 * Extract range information for a given file slice
 */
public class HoodieRangeInfoHandle<T extends HoodieRecordPayload> extends HoodieReadHandle<T> {

  public HoodieRangeInfoHandle(HoodieWriteConfig config, HoodieTable<T> hoodieTable,
      Pair<String, String> partitionPathFilePair) {
    super(config, null, hoodieTable, partitionPathFilePair);
  }

  public String[] getMinMaxKeys() {
    HoodieDataFile dataFile = getLatestDataFile();
    return ParquetUtils.readMinMaxRecordKeys(hoodieTable.getHadoopConf(), new Path(dataFile.getPath()));
  }
}
