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

import org.apache.hudi.avro.ConvertingGenericData;
import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.SchemaOnReadEvolutionTestUtils;
import org.apache.hudi.common.testutils.SchemaOnWriteEvolutionTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieFileGroupReaderOnJava extends HoodieFileGroupReaderOnJavaTestBase<IndexedRecord> {
  private static final StorageConfiguration<?> STORAGE_CONFIGURATION = new HadoopStorageConfiguration(false);

  @Override
  public StorageConfiguration<?> getStorageConf() {
    return STORAGE_CONFIGURATION;
  }

  @Override
  public HoodieReaderContext<IndexedRecord> getHoodieReaderContext(
      String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf, HoodieTableMetaClient metaClient) {
    return new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty());
  }

  @Override
  public void assertRecordsEqual(Schema schema, IndexedRecord expected, IndexedRecord actual) {
    assertEquals(expected, actual);
  }

  @Override
  public void assertRecordMatchesSchema(Schema schema, IndexedRecord record) {
    assertTrue(ConvertingGenericData.INSTANCE.validate(schema, record));
  }

  @Override
  public SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs getSchemaOnWriteConfigs() {
    return new SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs();
  }

  @Override
  public SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs getSchemaOnReadConfigs() {
    SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs configs = new SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs();
    configs.addNewFieldSupport = false;
    configs.addNewFieldNotAtEndSupport = false;
    configs.renameColumnSupport = false;
    configs.renameColumnAsPreviouslyRemovedSupport = false;

    configs.intToLongSupport = false;
    configs.intToFloatSupport = false;
    configs.intToDoubleSupport = false;
    configs.intToStringSupport = false;
    configs.intToDecimalFixedSupport = false;
    configs.intToDecimalBytesSupport = false;

    configs.longToFloatSupport = false;
    configs.longToDoubleSupport = false;
    configs.longToStringSupport = false;
    configs.longToDecimalFixedSupport = false;
    configs.longToDecimalBytesSupport = false;

    configs.floatToDoubleSupport = false;
    configs.floatToStringSupport = false;
    configs.floatToDecimalFixedSupport = false;
    configs.floatToDecimalBytesSupport = false;

    configs.doubleToStringSupport = false;
    configs.doubleToDecimalFixedSupport = false;
    configs.doubleToDecimalBytesSupport = false;

    configs.stringToDecimalFixedSupport = false;
    configs.stringToDecimalBytesSupport = false;
    configs.stringToDateSupport = false;

    configs.decimalFixedToStringSupport = false;
    configs.decimalBytesToStringSupport = false;

    configs.dateToStringSupport = false;
    return configs;
  }
}
