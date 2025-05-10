/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;

import java.util.List;

/**
 * Engine-specific record generator for expression index
 */
public interface ExpressionIndexRecordGenerator {
  EngineType getEngineType();

  /**
   * Generates expression index records
   *
   * @param filesToIndex    files to index containing the information of file path, file size and partition name
   * @param indexDefinition definition of the expression index for which records need to be generated
   * @param metaClient      {@link HoodieTableMetaClient} isntance
   * @param parallelism     parallelism to use for engine operations
   * @param readerSchema    reader schema
   * @param storageConf     storage config
   * @param instantTime     instant time
   * @return expression index records
   */
  HoodieData<HoodieRecord> generate(
      List<FileToIndex> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism, Schema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime);

  class FileToIndex {
    private final String partition;
    private final String path;
    private final long size;

    private FileToIndex(String partition, String path, long size) {
      this.partition = partition;
      this.path = path;
      this.size = size;
    }

    public static FileToIndex of(String partition, String path, long size) {
      return new FileToIndex(partition, path, size);
    }

    public String partition() {
      return partition;
    }

    public String path() {
      return path;
    }

    public long size() {
      return size;
    }
  }
}
