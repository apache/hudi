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

package org.apache.hudi.common.table;

import org.apache.hudi.AvroParquetAdapter;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

public class ParquetTableSchemaResolver extends TableSchemaResolver {
  private static final AvroParquetAdapter ADAPTER = AvroParquetAdapter.getAdapter();

  public ParquetTableSchemaResolver(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  public static MessageType convertAvroSchemaToParquet(Schema schema, Configuration hadoopConf) {
    return ADAPTER.getAvroSchemaConverter(new HadoopStorageConfiguration(hadoopConf)).convert(schema);
  }

  private Schema convertParquetSchemaToAvro(MessageType parquetSchema) {
    return ADAPTER.getAvroSchemaConverter(metaClient.getStorageConf()).convert(parquetSchema);
  }

  private MessageType convertAvroSchemaToParquet(Schema schema) {
    return ADAPTER.getAvroSchemaConverter(metaClient.getStorageConf()).convert(schema);
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Parquet format.
   *
   * @return Parquet schema for the table
   */
  public MessageType getTableParquetSchema() throws Exception {
    return convertAvroSchemaToParquet(getTableAvroSchema(true));
  }

  /**
   * Gets users data schema for a hoodie table in Parquet format.
   *
   * @return Parquet schema for the table
   */
  public MessageType getTableParquetSchema(boolean includeMetadataField) throws Exception {
    return convertAvroSchemaToParquet(getTableAvroSchema(includeMetadataField));
  }

}
