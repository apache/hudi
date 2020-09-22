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

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;

import java.util.Iterator;

import scala.Tuple2;

/**
 * {@link HoodieRecordLocation} fetch handle for all records from {@link HoodieBaseFile} of interest.
 *
 * @param <T>
 */
public class HoodieKeyLocationFetchHandle<T extends HoodieRecordPayload, I, K, O, P> extends HoodieReadHandle<T, I, K, O, P> {

  private final Pair<String, HoodieBaseFile> partitionPathBaseFilePair;

  public HoodieKeyLocationFetchHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O, P> hoodieTable,
                                      Pair<String, HoodieBaseFile> partitionPathBaseFilePair) {
    super(config, null, hoodieTable, Pair.of(partitionPathBaseFilePair.getLeft(), partitionPathBaseFilePair.getRight().getFileId()));
    this.partitionPathBaseFilePair = partitionPathBaseFilePair;
  }

  public Iterator<Tuple2<HoodieKey, HoodieRecordLocation>> locations() {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    return ParquetUtils.fetchRecordKeyPartitionPathFromParquet(hoodieTable.getHadoopConf(), new Path(baseFile.getPath())).stream()
        .map(entry -> new Tuple2<>(entry,
            new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId()))).iterator();
  }
}
