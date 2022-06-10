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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
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

  @Override
  protected FileSystem getFileSystem() {
    return hoodieTable.getMetaClient().getFs();
  }

  public Pair<String, String> getPartitionPathFileIDPair() {
    return partitionPathFileIDPair;
  }

  public String getFileId() {
    return partitionPathFileIDPair.getRight();
  }

  protected HoodieBaseFile getLatestDataFile() {
    return hoodieTable.getBaseFileOnlyView()
        .getLatestBaseFile(partitionPathFileIDPair.getLeft(), partitionPathFileIDPair.getRight()).get();
  }

  protected HoodieAvroFileReader createNewFileReader() throws IOException {
    return HoodieFileReaderFactory.getFileReader(hoodieTable.getHadoopConf(),
        new Path(getLatestDataFile().getPath()));
  }
}
