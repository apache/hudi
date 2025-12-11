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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;

/**
 * Base class for read operations done logically on the file group.
 */
public abstract class HoodieReadHandle<T, I, K, O> extends HoodieIOHandle<T, I, K, O> {

  protected final Pair<String, String> partitionPathFileIDPair;

  public HoodieReadHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                          Pair<String, String> partitionPathFileIDPair) {
    super(config, Option.empty(), hoodieTable);
    this.partitionPathFileIDPair = partitionPathFileIDPair;
  }

  public HoodieReadHandle(HoodieWriteConfig config,
                          Option<String> instantTime,
                          HoodieTable<T, I, K, O> hoodieTable,
                          Pair<String, String> partitionPathFileIDPair) {
    super(config, instantTime, hoodieTable);
    this.partitionPathFileIDPair = partitionPathFileIDPair;
  }

  @Override
  public HoodieStorage getStorage() {
    return hoodieTable.getStorage();
  }

  public Pair<String, String> getPartitionPathFileIDPair() {
    return partitionPathFileIDPair;
  }

  public String getFileId() {
    return partitionPathFileIDPair.getRight();
  }

  protected HoodieBaseFile getLatestBaseFile() {
    return hoodieTable.getBaseFileOnlyView().getBaseFileOn(partitionPathFileIDPair.getLeft(), instantTime, partitionPathFileIDPair.getRight()).get();
  }

  protected HoodieFileReader createNewFileReader() throws IOException {
    return HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getReaderFactory(this.config.getRecordMerger().getRecordType())
        .getFileReader(config, getLatestBaseFile().getStoragePath());
  }

  protected HoodieFileReader createNewFileReader(HoodieBaseFile hoodieBaseFile) throws IOException {
    return HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getReaderFactory(this.config.getRecordMerger().getRecordType())
        .getFileReader(config, hoodieBaseFile.getStoragePath());
  }
}
