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

package org.apache.parquet.avro;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

/**
 * See org.apache.parquet.avro.AvroParquetReader.Builder.
 * We use HoodieAvroParquetReaderBuilder to support reading avro from non-legacy map/list in parquet file.
 * Not supported AvroParquetReader.Builder#withDataModel and this api is not used by hudi currently.
 */
public class HoodieAvroParquetReaderBuilder<T> extends ParquetReader.Builder<T> {

  public HoodieAvroParquetReaderBuilder(Path path) {
    super(path);
  }

  public HoodieAvroParquetReaderBuilder(InputFile file) {
    super(file);
  }

  @Override
  protected ReadSupport<T> getReadSupport() {
    // see org.apache.parquet.avro.AvroParquetReader.Builder#getReadSupport
    // AVRO_COMPATIBILITY default set to false
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    return new HoodieAvroReadSupport<>();
  }
}
