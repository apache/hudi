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
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
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

  public ClosableIterator<Pair<HoodieKey, HoodieRecordLocation>> locations() {
    return locations(hoodieTable.getStorage(), partitionPathBaseFilePair, keyGeneratorOpt);
  }

  public ClosableIterator<Pair<String, HoodieRecordGlobalLocation>> globalLocations() {
    return globalLocations(hoodieTable.getStorage(), partitionPathBaseFilePair, keyGeneratorOpt);
  }

  /**
   * Returns the keys of the records in a base file with their locations.
   *
   * @param storage                   storage to read the base file with
   * @param partitionPathBaseFilePair partition path and base file
   * @param keyGeneratorOpt           key generator, when the table does not populate the meta fields
   */
  public static ClosableIterator<Pair<HoodieKey, HoodieRecordLocation>> locations(
      HoodieStorage storage, Pair<String, HoodieBaseFile> partitionPathBaseFilePair, Option<BaseKeyGenerator> keyGeneratorOpt) {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    String commitTime = baseFile.getCommitTime();
    String fileId = baseFile.getFileId();
    return new CloseableMappingIterator<>(fetchRecordKeysWithPositions(storage, partitionPathBaseFilePair, keyGeneratorOpt),
        entry -> Pair.of(entry.getLeft(), new HoodieRecordLocation(commitTime, fileId, entry.getRight())));
  }

  /**
   * Returns the record keys in a base file with their global locations.
   *
   * @param storage                   storage to read the base file with
   * @param partitionPathBaseFilePair partition path and base file
   * @param keyGeneratorOpt           key generator, when the table does not populate the meta fields
   */
  public static ClosableIterator<Pair<String, HoodieRecordGlobalLocation>> globalLocations(
      HoodieStorage storage, Pair<String, HoodieBaseFile> partitionPathBaseFilePair, Option<BaseKeyGenerator> keyGeneratorOpt) {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    return new CloseableMappingIterator<>(fetchRecordKeysWithPositions(storage, partitionPathBaseFilePair, keyGeneratorOpt),
        entry -> Pair.of(entry.getLeft().getRecordKey(),
            new HoodieRecordGlobalLocation(
                entry.getLeft().getPartitionPath(), baseFile.getCommitTime(),
                baseFile.getFileId(), entry.getRight())));
  }

  private static ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(
      HoodieStorage storage, Pair<String, HoodieBaseFile> partitionPathBaseFilePair, Option<BaseKeyGenerator> keyGeneratorOpt) {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(baseFile.getStoragePath());
    return fileFormatUtils.fetchRecordKeysWithPositions(storage, baseFile.getStoragePath(), keyGeneratorOpt, Option.of(partitionPathBaseFilePair.getKey()));
  }
}
