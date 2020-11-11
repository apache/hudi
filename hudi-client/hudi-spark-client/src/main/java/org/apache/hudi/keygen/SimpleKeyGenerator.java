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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;

import java.util.Collections;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleKeyGenerator extends BuiltinKeyGenerator {

  private final SimpleAvroKeyGenerator simpleAvroKeyGenerator;

  public SimpleKeyGenerator(TypedProperties props) {
    this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
  }

  SimpleKeyGenerator(TypedProperties props, String partitionPathField) {
    this(props, null, partitionPathField);
  }

  SimpleKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField) {
    super(props);
    this.recordKeyFields = recordKeyField == null
        ? Collections.emptyList()
        : Collections.singletonList(recordKeyField);
    this.partitionPathFields = Collections.singletonList(partitionPathField);
    simpleAvroKeyGenerator = new SimpleAvroKeyGenerator(props, recordKeyField, partitionPathField);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return simpleAvroKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return simpleAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    buildFieldPositionMapIfNeeded(row.schema());
    return RowKeyGeneratorHelper.getRecordKeyFromRow(row, getRecordKeyFields(), recordKeyPositions, false);
  }

  @Override
  public String getPartitionPath(Row row) {
    buildFieldPositionMapIfNeeded(row.schema());
    return RowKeyGeneratorHelper.getPartitionPathFromRow(row, getPartitionPathFields(),
        hiveStylePartitioning, partitionPathPositions);
  }
}
