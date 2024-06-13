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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class HoodieFileGroupReaderTestUtils {
  public static HoodieFileGroupReader<IndexedRecord> createFileGroupReader(
      Option<FileSlice> fileSliceOpt,
      String basePath,
      String latestCommitTime,
      Schema schema,
      boolean shouldUseRecordPosition,
      long start,
      long length,
      TypedProperties properties,
      HoodieStorage storage,
      HoodieTableConfig tableConfig,
      HoodieReaderContext<IndexedRecord> readerContext,
      HoodieTableMetaClient metaClient
  ) {
    assert (fileSliceOpt.isPresent());
    return new HoodieFileGroupReaderBuilder()
        .withReaderContext(readerContext)
        .withStorage(storage)
        .withFileSlice(fileSliceOpt.get())
        .withStart(start)
        .withLength(length)
        .withProperties(properties)
        .withTableConfig(tableConfig)
        .build(basePath, latestCommitTime, schema, shouldUseRecordPosition, metaClient);
  }

  public static class HoodieFileGroupReaderBuilder {
    private HoodieReaderContext<IndexedRecord> readerContext;
    private FileSlice fileSlice;
    private HoodieStorage storage;
    private TypedProperties props;
    private long start;
    private long length;
    private HoodieTableConfig tableConfig;

    public HoodieFileGroupReaderBuilder withReaderContext(
        HoodieReaderContext<IndexedRecord> context) {
      this.readerContext = context;
      return this;
    }

    public HoodieFileGroupReaderBuilder withFileSlice(FileSlice fileSlice) {
      this.fileSlice = fileSlice;
      return this;
    }

    public HoodieFileGroupReaderBuilder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public HoodieFileGroupReaderBuilder withProperties(TypedProperties props) {
      this.props = props;
      return this;
    }

    public HoodieFileGroupReaderBuilder withStart(long start) {
      this.start = start;
      return this;
    }

    public HoodieFileGroupReaderBuilder withLength(long length) {
      this.length = length;
      return this;
    }

    public HoodieFileGroupReaderBuilder withTableConfig(
        HoodieTableConfig tableConfig
    ) {
      this.tableConfig = tableConfig;
      return this;
    }

    public HoodieFileGroupReader<IndexedRecord> build(
        String basePath,
        String latestCommitTime,
        Schema schema,
        boolean shouldUseRecordPosition,
        HoodieTableMetaClient metaClient
    ) {
      props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(),String.valueOf(1024 * 1024 * 1000));
      props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(),  basePath + "/" + HoodieTableMetaClient.TEMPFOLDER_NAME);
      props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), ExternalSpillableMap.DiskMapType.ROCKS_DB.name());
      props.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), "false");
      return new HoodieFileGroupReader<>(
          readerContext,
          storage,
          basePath,
          latestCommitTime,
          fileSlice,
          schema,
          schema,
          Option.empty(),
          metaClient,
          props,
          tableConfig,
          start,
          length,
          shouldUseRecordPosition);
    }
  }
}
