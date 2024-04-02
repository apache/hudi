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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;

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
      Configuration hadoopConf,
      HoodieTableConfig tableConfig,
      HoodieReaderContext<IndexedRecord> readerContext
  ) {
    assert (fileSliceOpt.isPresent());
    return new HoodieFileGroupReaderBuilder()
        .withReaderContext(readerContext)
        .withHadoopConf(hadoopConf)
        .withFileSlice(fileSliceOpt.get())
        .withStart(start)
        .withLength(length)
        .withProperties(properties)
        .withTableConfig(tableConfig)
        .build(basePath, latestCommitTime, schema, shouldUseRecordPosition);
  }

  public static class HoodieFileGroupReaderBuilder {
    private HoodieReaderContext<IndexedRecord> readerContext;
    private FileSlice fileSlice;
    private Configuration hadoopConf;
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

    public HoodieFileGroupReaderBuilder withHadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
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
        boolean shouldUseRecordPosition
    ) {
      return new HoodieFileGroupReader<>(
          readerContext,
          hadoopConf,
          basePath,
          latestCommitTime,
          fileSlice,
          schema,
          schema,
          Option.empty(),
          null,
          props,
          tableConfig,
          start,
          length,
          shouldUseRecordPosition,
          1024 * 1024 * 1000,
          basePath + "/" + HoodieTableMetaClient.TEMPFOLDER_NAME,
          ExternalSpillableMap.DiskMapType.ROCKS_DB,
          false);
    }
  }
}