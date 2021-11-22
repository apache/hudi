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

package org.apache.hudi.metadata;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieHFileReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * HFile reader for the Metadata table log files. Metadata table records in the
 * HFile data blocks have the redundant key field in the record payload trimmed.
 * So, when the log reader is reading records, materialization of such trimmed
 * records must be done before handing the records to the callers. This class
 * takes care of Metadata table record materialization, any needed.
 *
 * @param <R> Metadata table record type.
 */
public class HoodieMetadataHFileReader<R extends IndexedRecord> extends HoodieHFileReader<R> {
  public HoodieMetadataHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig) throws IOException {
    super(configuration, path, cacheConfig);
  }

  public HoodieMetadataHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig, FileSystem inlineFs,
                                   String keyField) throws IOException {
    super(configuration, path, cacheConfig, inlineFs, keyField);
  }

  public HoodieMetadataHFileReader(final byte[] content, final String keyField) throws IOException {
    super(content, keyField);
  }

  /**
   * @param keyField
   * @param keyBytes
   * @param record
   */
  @Override
  protected void materializeRecordIfNeeded(final Option<String> keyField, final ByteBuffer keyBytes, R record) {
    if (!keyField.isPresent()) {
      return;
    }

    Schema.Field keySchemaField = record.getSchema().getField(keyField.get());
    record.put(keySchemaField.pos(), new String(keyBytes.array()));

    keySchemaField = record.getSchema().getField(keyField.get());
    if (keySchemaField != null) {
      record.put(keySchemaField.pos(), new String(keyBytes.array()));
    }
  }
}
