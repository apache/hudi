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
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;

import java.util.Iterator;

/**
 * {@link HoodieRecordLocation} fetch handle for all records from {@link HoodieBaseFile} of interest.
 */
public class HoodieKeyLocationFetchHandle {

  private final Pair<String, HoodieBaseFile> partitionPathBaseFilePair;
  private final Option<BaseKeyGenerator> keyGeneratorOpt;
  private final HoodieStorage storage;

  public HoodieKeyLocationFetchHandle(HoodieStorage storage, Pair<String, HoodieBaseFile> partitionPathBaseFilePair, Option<BaseKeyGenerator> keyGeneratorOpt) {
    this.partitionPathBaseFilePair = partitionPathBaseFilePair;
    this.keyGeneratorOpt = keyGeneratorOpt;
    this.storage = storage;
  }

  private Iterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieBaseFile baseFile) {
    FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(baseFile.getStoragePath());
    if (keyGeneratorOpt.isPresent()) {
      return fileFormatUtils.fetchRecordKeysWithPositions(storage, baseFile.getStoragePath(), keyGeneratorOpt);
    } else {
      return fileFormatUtils.fetchRecordKeysWithPositions(storage, baseFile.getStoragePath());
    }
  }

  public Iterator<Pair<HoodieKey, HoodieRecordLocation>> locations() {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    String commitTime = baseFile.getCommitTime();
    String fileId = baseFile.getFileId();
    return new MappingIterator<>(fetchRecordKeysWithPositions(baseFile),
        entry -> Pair.of(entry.getLeft(), new HoodieRecordLocation(commitTime, fileId, entry.getRight())));
  }

  public Iterator<Pair<String, HoodieRecordGlobalLocation>> globalLocations() {
    HoodieBaseFile baseFile = partitionPathBaseFilePair.getRight();
    return new MappingIterator<>(fetchRecordKeysWithPositions(baseFile),
        entry -> Pair.of(entry.getLeft().getRecordKey(),
            new HoodieRecordGlobalLocation(entry.getLeft().getPartitionPath(), baseFile.getCommitTime(), baseFile.getFileId(), entry.getRight())));
  }
}
