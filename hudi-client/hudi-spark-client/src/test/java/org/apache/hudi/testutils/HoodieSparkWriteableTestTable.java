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

package org.apache.hudi.testutils;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HoodieSparkWriteableTestTable extends HoodieWriteableTestTable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkWriteableTestTable.class);

  private HoodieSparkWriteableTestTable(String basePath, HoodieStorage storage,
                                        HoodieTableMetaClient metaClient, Schema schema,
                                        BloomFilter filter,
                                        HoodieTableMetadataWriter metadataWriter) {
    this(basePath, storage, metaClient, schema, filter, metadataWriter, Option.empty());
  }

  private HoodieSparkWriteableTestTable(String basePath, HoodieStorage storage,
                                        HoodieTableMetaClient metaClient, Schema schema,
                                        BloomFilter filter,
                                        HoodieTableMetadataWriter metadataWriter,
                                        Option<HoodieEngineContext> context) {
    super(basePath, storage, metaClient, schema, filter, metadataWriter, context);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema,
                                                 BloomFilter filter) {
    return of(metaClient, schema, filter, Option.empty());
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter, Option<HoodieEngineContext> context) {
    return new HoodieSparkWriteableTestTable(metaClient.getBasePath().toString(),
        metaClient.getRawStorage(),
        metaClient, schema, filter, null, context);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter,
                                                 HoodieTableMetadataWriter metadataWriter) {
    return of(metaClient, schema, filter, metadataWriter, Option.empty());
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter,
                                                 HoodieTableMetadataWriter metadataWriter, Option<HoodieEngineContext> context) {
    return new HoodieSparkWriteableTestTable(metaClient.getBasePath().toString(),
        metaClient.getRawStorage(),
        metaClient, schema, filter, metadataWriter, context);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema) {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    return of(metaClient, schema, filter);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema,
                                                 HoodieTableMetadataWriter metadataWriter) {
    return of(metaClient, schema, metadataWriter, Option.empty());
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema,
                                                 HoodieTableMetadataWriter metadataWriter, Option<HoodieEngineContext> context) {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.DYNAMIC_V0.name());
    return of(metaClient, schema, filter, metadataWriter, context);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTable hoodieTable, Schema schema) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTable hoodieTable, Schema schema, BloomFilter filter) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema, filter);
  }

  @Override
  public HoodieSparkWriteableTestTable addCommit(String instantTime) throws Exception {
    return (HoodieSparkWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public HoodieSparkWriteableTestTable forCommit(String instantTime) {
    return (HoodieSparkWriteableTestTable) super.forCommit(instantTime);
  }

  public String getFileIdWithInserts(String partition) throws Exception {
    return getFileIdWithInserts(partition, new HoodieRecord[0]);
  }

  public String getFileIdWithInserts(String partition, HoodieRecord... records) throws Exception {
    return getFileIdWithInserts(partition, Arrays.asList(records));
  }

  public String getFileIdWithInserts(String partition, List<HoodieRecord> records) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withInserts(partition, fileId, records);
    return fileId;
  }

  public HoodieSparkWriteableTestTable withInserts(String partition, String fileId) throws Exception {
    return withInserts(partition, fileId, new HoodieRecord[0]);
  }

  public HoodieSparkWriteableTestTable withInserts(String partition, String fileId, HoodieRecord... records) throws Exception {
    withInserts(partition, fileId, Arrays.asList(records));
    return this;
  }

  public StoragePath withInserts(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    return super.withInserts(partition, fileId, records, new SparkTaskContextSupplier());
  }

  public HoodieSparkWriteableTestTable withLogAppends(String partition, String fileId, HoodieRecord... records) throws Exception {
    withLogAppends(partition, fileId, Arrays.asList(records));
    return this;
  }

  public Map<String, List<HoodieLogFile>> withLogAppends(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    return super.withLogAppends(partition, fileId, records);
  }
}
