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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataUtil.ColumnStatsCollector;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/** Connects Lance writers to the column statistics carried by their write status. */
public final class ColumnStatsStreamingTracker {
  private ColumnStatsStreamingTracker() {
  }

  public static Option<ColumnStatsCollector> createCollector(HoodieFileWriter<?> writer,
                                                            HoodieWriteConfig config,
                                                            HoodieTableMetaClient metaClient,
                                                            Supplier<HoodieSchema> schemaSupplier,
                                                            String fileName,
                                                            HoodieRecordType recordType) {
    if (!config.isMetadataColumnStatsIndexEnabled()
        || metaClient.getTableConfig().getBaseFileFormat() != HoodieFileFormat.LANCE) {
      return Option.empty();
    }

    HoodieSchema schema = schemaSupplier.get();
    HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, metaClient);
    List<String> columnsToIndex = new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(
        metaClient.getTableConfig(), config.getMetadataConfig(), Lazy.eagerly(Option.of(schema)),
        Option.of(recordType), indexVersion).keySet());
    List<Pair<String, HoodieSchemaField>> fieldsToIndex = columnsToIndex.stream()
        .map(column -> HoodieSchemaUtils.getNestedField(schema, column))
        .filter(Option::isPresent)
        .map(Option::get)
        .collect(Collectors.toList());
    if (fieldsToIndex.isEmpty()) {
      return Option.empty();
    }

    StorageConfiguration<?> storageConfig = metaClient.getStorageConf().newInstance();
    storageConfig.set(HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), String.valueOf(config.getBooleanOrDefault(HoodieStorageConfig.WRITE_UTC_TIMEZONE)));
    ColumnStatsCollector collector = new ColumnStatsCollector(fieldsToIndex, fileName, schema, storageConfig, indexVersion);
    return writer.setColumnStatsCollector(collector::collect) ? Option.of(collector) : Option.empty();
  }
}
