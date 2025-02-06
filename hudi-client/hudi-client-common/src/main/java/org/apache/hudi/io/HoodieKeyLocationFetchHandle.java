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
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

/**
 * {@link HoodieRecordLocation} fetch handle for all records from {@link HoodieBaseFile} of interest.
 *
 * @param <T>
 */
public class HoodieKeyLocationFetchHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  private final Pair<String, HoodieBaseFile> partitionPathBaseFilePair;
  private final Option<BaseKeyGenerator> keyGeneratorOpt;

  public HoodieKeyLocationFetchHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                                      Pair<String, HoodieBaseFile> partitionPathBaseFilePair, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, hoodieTable, Pair.of(partitionPathBaseFilePair.getLeft(), partitionPathBaseFilePair.getRight().getFileId()));
    this.partitionPathBaseFilePair = partitionPathBaseFilePair;
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  private ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieBaseFile baseFile) {
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getFileFormatUtils(baseFile.getStoragePath());
    return fileFormatUtils.fetchRecordKeysWithPositions(hoodieTable.getStorage(), baseFile.getStoragePath(), keyGeneratorOpt, Option.of(partitionPathBaseFilePair.getKey()));
  }

  public ClosableIterator<Pair<HoodieKey, HoodieRecordLocation>> locations() {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    String commitTime = baseFile.getCommitTime();
    String fileId = baseFile.getFileId();
    return new CloseableMappingIterator<>(fetchRecordKeysWithPositions(baseFile),
        entry -> Pair.of(entry.getLeft(), new HoodieRecordLocation(commitTime, fileId, entry.getRight())));
  }

  public ClosableIterator<Pair<String, HoodieRecordGlobalLocation>> globalLocations() {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    return new CloseableMappingIterator<>(fetchRecordKeysWithPositions(baseFile),
        entry -> Pair.of(entry.getLeft().getRecordKey(),
            new HoodieRecordGlobalLocation(
                entry.getLeft().getPartitionPath(), baseFile.getCommitTime(),
                baseFile.getFileId(), entry.getRight())));
  }
}
