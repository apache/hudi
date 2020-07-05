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

package org.apache.hudi.keygen;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;

/**
 * Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class ComplexKeyGenerator extends BuiltinKeyGenerator {

  public static final String DEFAULT_RECORD_KEY_SEPARATOR = ":";
  protected final boolean hiveStylePartitioning;
  protected final boolean encodePartitionPath;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.setRecordKeyFields(Arrays.stream(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY())
        .split(",")).map(String::trim).collect(Collectors.toList()));
    this.setPartitionPathFields(Arrays.stream(props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY())
        .split(",")).map(String::trim).collect(Collectors.toList()));
    this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
    this.encodePartitionPath = props.getBoolean(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL()));
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return KeyGenUtils.getRecordKey(record, getRecordKeyFields());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getRecordPartitionPath(record, getPartitionPathFields(), hiveStylePartitioning, encodePartitionPath);
  }

  @Override
  public String getRecordKeyFromRow(Row row) {
    return RowKeyGeneratorHelper.getRecordKeyFromRow(row, getRecordKeyFields(), getRecordKeyPositions(), true);
  }

  @Override
  public String getPartitionPathFromRow(Row row) {
    return RowKeyGeneratorHelper.getPartitionPathFromRow(row, getPartitionPathFields(),
        hiveStylePartitioning, getPartitionPathPositions());
  }
}