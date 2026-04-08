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

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

/**
 * Copy from org.apache.parquet.avro.AvroParquetReader.Builder.
 * We use HoodieAvroParquetReaderBuilder to build HoodieAvroReadSupport
 * that can support reading avro from non-legacy map/list in parquet file.
 */
public class HoodieAvroParquetReaderBuilder<T> extends ParquetReader.Builder<T> {

  private GenericData model = null;
  private boolean enableCompatibility = true;
  private boolean isReflect = true;
  private boolean isLogicalTimestampRepairNeeded;
  private Schema tableSchema = null;

  @Deprecated
  public HoodieAvroParquetReaderBuilder(Path path, boolean isLogicalTimestampRepairNeeded) {
    super(path);
    this.isLogicalTimestampRepairNeeded = isLogicalTimestampRepairNeeded;
  }

  public HoodieAvroParquetReaderBuilder(InputFile file) {
    super(file);
  }

  public HoodieAvroParquetReaderBuilder<T> withDataModel(GenericData model) {
    this.model = model;

    // only generic and specific are supported by AvroIndexedRecordConverter
    if (model.getClass() != GenericData.class
        && model.getClass() != SpecificData.class) {
      isReflect = true;
    }

    return this;
  }

  public HoodieAvroParquetReaderBuilder<T> disableCompatibility() {
    this.enableCompatibility = false;
    return this;
  }

  public HoodieAvroParquetReaderBuilder<T> withCompatibility(boolean enableCompatibility) {
    this.enableCompatibility = enableCompatibility;
    return this;
  }

  public HoodieAvroParquetReaderBuilder<T> withTableSchema(Schema tableSchema) {
    this.tableSchema = tableSchema;
    return this;
  }

  @Override
  protected ReadSupport<T> getReadSupport() {
    if (isReflect) {
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    } else {
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, enableCompatibility);
    }
    return new HoodieAvroReadSupport<>(model, Option.ofNullable(tableSchema).map(schema -> getAvroSchemaConverter(conf).convert(schema)),
        isLogicalTimestampRepairNeeded);
  }
}
