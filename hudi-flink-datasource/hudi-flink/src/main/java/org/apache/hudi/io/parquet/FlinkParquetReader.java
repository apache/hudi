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

package org.apache.hudi.io.parquet;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.io.storage.row.RowDataFileReader;
import org.apache.hudi.table.expression.ExpressionPredicates.Predicate;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.InternalSchemaManager;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * An {@link RowDataFileReader} to get RowData iterator from Parquet files and Parquet data blocks.
 */
public class FlinkParquetReader implements RowDataFileReader {
  private final InternalSchemaManager internalSchemaManager;
  private final Configuration conf;

  private static final int DEFAULT_BATCH_SIZE = 2048;

  public FlinkParquetReader(
      InternalSchemaManager internalSchemaManager,
      Configuration conf) {
    this.internalSchemaManager = internalSchemaManager;
    this.conf = conf;
  }

  @Override
  public ClosableIterator<RowData> getRowDataIterator(
      List<String> fieldNames,
      List<DataType> fieldTypes,
      int[] selectedFields,
      List<Predicate> predicates,
      Path path,
      long start,
      long length) throws IOException {
    final boolean useUTCTimeStamp = conf.getBoolean(
        HoodieStorageConfig.PARQUET_READ_UTC_TIMEZONE.key(),
        HoodieStorageConfig.PARQUET_READ_UTC_TIMEZONE.defaultValue());

    LinkedHashMap<String, Object> partitionSpec = FilePathUtils.generatePartitionSpecs(
        path.toString(),
        fieldNames,
        fieldTypes,
        internalSchemaManager.getFlinkConf().get(FlinkOptions.PARTITION_DEFAULT_NAME),
        internalSchemaManager.getFlinkConf().get(FlinkOptions.PARTITION_PATH_FIELD),
        internalSchemaManager.getFlinkConf().get(FlinkOptions.HIVE_STYLE_PARTITIONING));

    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        useUTCTimeStamp,
        true,
        conf,
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        partitionSpec,
        selectedFields,
        DEFAULT_BATCH_SIZE,
        new org.apache.flink.core.fs.Path(path.toUri()),
        start,
        length,
        predicates);
  }
}
