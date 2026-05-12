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

package org.apache.hudi.table.format;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

/**
 * Factory methods to create RowData file reader.
 *
 * <p>The {@code schemaOption} parameter supplied by the caller flows through to the
 * {@link HoodieRowDataParquetReader} constructor. When present, the reader uses the
 * {@link HoodieSchema} to derive a Flink RowType that preserves Hudi logical types
 * (Variant, Blob, Vector). When absent, the reader falls back to inferring the RowType
 * from the Parquet physical schema.
 */
public class HoodieRowDataFileReaderFactory extends HoodieFileReaderFactory {
  public HoodieRowDataFileReaderFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  protected HoodieFileReader newParquetFileReader(StoragePath path) {
    return new HoodieRowDataParquetReader(storage, path, Option.empty());
  }

  @Override
  protected HoodieFileReader newParquetFileReader(StoragePath path, Option<HoodieSchema> schemaOption) {
    return new HoodieRowDataParquetReader(storage, path, schemaOption);
  }
}
