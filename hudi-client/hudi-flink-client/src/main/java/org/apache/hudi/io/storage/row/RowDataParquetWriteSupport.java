/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.io.storage.row.parquet.ParquetRowDataWriter;
import org.apache.hudi.io.storage.row.parquet.ParquetSchemaConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

/**
 * Row data parquet write support.
 */
public class RowDataParquetWriteSupport extends WriteSupport<RowData> {

  private final RowType rowType;
  private final MessageType schema;
  private ParquetRowDataWriter writer;
  protected final Configuration hadoopConf;

  public RowDataParquetWriteSupport(RowType rowType, Configuration config) {
    super();
    this.rowType = rowType;
    this.hadoopConf = new Configuration(config);
    this.schema = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", rowType);
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(schema, new HashMap<>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    // should make the utc timestamp configurable
    boolean utcTimestamp =
        hadoopConf.getBoolean(
            HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(),
            HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue());
    this.writer = new ParquetRowDataWriter(recordConsumer, rowType, schema, utcTimestamp);
  }

  @Override
  public void write(RowData record) {
    try {
      this.writer.write(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
