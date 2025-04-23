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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.storage.StorageConfiguration;

/**
 * A factory to create {@link RowDataFileReader} to
 */
public class RowDataFileReaderFactories {

  public static Factory getFactory(HoodieFileFormat format) {
    switch (format) {
      case PARQUET:
        return new ParquetFileReaderFactory();
      default:
        throw new UnsupportedOperationException(String.format("RowData file reader for format: %s is not supported yet.", format));
    }
  }

  private static class ParquetFileReaderFactory implements Factory {
    @Override
    public RowDataFileReader createFileReader(InternalSchemaManager internalSchemaManager, StorageConfiguration<?> conf) {
      return new FlinkParquetReader(internalSchemaManager, conf);
    }
  }

  public interface Factory {
    /**
     * @param internalSchemaManager InternalSchema manager used for schema evolution
     * @param conf flink configuration
     * @return A RowData file reader
     */
    RowDataFileReader createFileReader(InternalSchemaManager internalSchemaManager, StorageConfiguration<?> conf);
  }
}
