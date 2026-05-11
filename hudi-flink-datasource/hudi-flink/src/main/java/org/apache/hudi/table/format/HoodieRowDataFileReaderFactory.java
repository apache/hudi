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

import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

/**
 * Factory methods to create RowData file reader.
 *
 * <p><b>Important:</b> The reader returned by this factory does not carry a HoodieSchema.
 * Callers <b>must</b> chain {@code .withTableSchema(hoodieSchema)} on the returned reader
 * before calling {@link HoodieRowDataParquetReader#getRowType()},
 * {@link HoodieRowDataParquetReader#getSchema()}, or
 * {@link HoodieRowDataParquetReader#getRecordKeyIterator()}.
 * An {@link IllegalStateException} is thrown otherwise.
 *
 * @see HoodieRowDataParquetReader#withTableSchema
 */
public class HoodieRowDataFileReaderFactory extends HoodieFileReaderFactory {
  public HoodieRowDataFileReaderFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  protected HoodieFileReader newParquetFileReader(StoragePath path) {
    return new HoodieRowDataParquetReader(storage, path);
  }
}
