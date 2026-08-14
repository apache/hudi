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

package org.apache.hudi.io.log.block;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.storage.StorageConfiguration;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * HoodieFlinkAvroDataBlock contains a list of records serialized using Avro. It is used with the Parquet base file format.
 */
public class HoodieFlinkAvroDataBlock extends HoodieAvroDataBlock {

  public HoodieFlinkAvroDataBlock(
      @Nonnull List<HoodieRecord> records,
      @Nonnull Map<HeaderMetadataType, String> header,
      @Nonnull String keyField) {
    super(records, header, keyField);
  }

  @Override
  protected Properties initProperties(StorageConfiguration<?> storageConfig) {
    Properties properties = new Properties();
    properties.setProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(),
        storageConfig.getString(HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    return properties;
  }
}
