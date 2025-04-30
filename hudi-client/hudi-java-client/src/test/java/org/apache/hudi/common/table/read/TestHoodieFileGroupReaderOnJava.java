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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieFileGroupReaderOnJava extends HoodieFileGroupReaderOnJavaTestBase<IndexedRecord> {
  private static final StorageConfiguration<?> STORAGE_CONFIGURATION = new HadoopStorageConfiguration(false);

  @Override
  public StorageConfiguration<?> getStorageConf() {
    return STORAGE_CONFIGURATION;
  }

  @Override
  public HoodieReaderContext<IndexedRecord> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf, HoodieTableMetaClient metaClient) {
    return new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig());
  }

  @Override
  public void bootstrapTable(List<HoodieRecord> recordList, Map<String, String> writeConfigs) {
    throw new HoodieNotSupportedException(
        "HUDI-8773: Not supporting bootstrap table testing at the file group reader layer in Java yet.");
  }

  @Override
  public void assertRecordsEqual(Schema schema, IndexedRecord expected, IndexedRecord actual) {
    assertEquals(expected, actual);
  }
}
