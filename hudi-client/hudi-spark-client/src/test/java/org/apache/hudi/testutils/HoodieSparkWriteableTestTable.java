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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HoodieSparkWriteableTestTable extends HoodieWriteableTestTable {
  private static final Logger LOG = LogManager.getLogger(HoodieSparkWriteableTestTable.class);

  private HoodieSparkWriteableTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient, Schema schema,
                                        BloomFilter filter, HoodieTableMetadataWriter metadataWriter) {
    super(basePath, fs, metaClient, schema, filter, metadataWriter);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    return new HoodieSparkWriteableTestTable(metaClient.getBasePath(), metaClient.getRawFs(),
        metaClient, schema, filter, null);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter,
                                                 HoodieTableMetadataWriter metadataWriter) {
    return new HoodieSparkWriteableTestTable(metaClient.getBasePath(), metaClient.getRawFs(),
        metaClient, schema, filter, metadataWriter);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema) {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    return of(metaClient, schema, filter);
  }

  public static HoodieSparkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema,
                                                 HoodieTableMetadataWriter metadataWriter) {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.DYNAMIC_V0.name());
    return of(metaClient, schema, filter, metadataWriter);
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

  public Path withInserts(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    return super.withInserts(partition, fileId, records, new SparkTaskContextSupplier());
  }

  public HoodieSparkWriteableTestTable withAppends(String partition, String fileId, HoodieRecord... records) throws Exception {
    withAppends(partition, fileId, Arrays.asList(records));
    return this;
  }

  public Map<String, List<HoodieLogFile>> withAppends(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    return super.withAppends(partition, fileId, records);
  }
}
