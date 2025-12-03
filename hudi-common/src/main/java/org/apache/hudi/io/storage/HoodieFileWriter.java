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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Properties;

public interface HoodieFileWriter extends AutoCloseable {
  boolean canWrite();

  void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException;

  void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException;

  void close() throws IOException;

  default void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema) throws IOException {
    writeWithMetadata(key, record, schema, CollectionUtils.emptyProps());
  }

  default void write(String recordKey, HoodieRecord record, HoodieSchema schema) throws IOException {
    write(recordKey, record, schema, CollectionUtils.emptyProps());
  }

  /**
   * Return metadata from the underlying format file, for example, return {@code ParquetMetadata} for Parquet files.
   * The returned format metadata will be used to generate column statistics, like {@code HoodieColumnRangeMetadata}.
   */
  default Object getFileFormatMetadata() {
    throw new UnsupportedOperationException("HoodieFileWriter#getFormatMetadata is unsupported by default.");
  }
}
