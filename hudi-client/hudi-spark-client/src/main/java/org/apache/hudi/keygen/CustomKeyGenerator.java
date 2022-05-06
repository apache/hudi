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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Collections;

/**
 * This is a generic implementation of KeyGenerator where users can configure record key as a single field or a combination of fields. Similarly partition path can be configured to have multiple
 * fields or only one field. This class expects value for prop "hoodie.datasource.write.partitionpath.field" in a specific format. For example:
 *
 * properties.put("hoodie.datasource.write.partitionpath.field", "field1:PartitionKeyType1,field2:PartitionKeyType2").
 *
 * The complete partition path is created as <value for field1 basis PartitionKeyType1>/<value for field2 basis PartitionKeyType2> and so on.
 *
 * Few points to consider: 1. If you want to customize some partition path field on a timestamp basis, you can use field1:timestampBased 2. If you simply want to have the value of your configured
 * field in the partition path, use field1:simple 3. If you want your table to be non partitioned, simply leave it as blank.
 *
 * RecordKey is internally generated using either SimpleKeyGenerator or ComplexKeyGenerator.
 */
public class CustomKeyGenerator extends BuiltinKeyGenerator {

  private final CustomAvroKeyGenerator customAvroKeyGenerator;

  public CustomKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = props.getStringList(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), ",",
        Collections.singletonList(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.defaultValue()));
    this.partitionPathFields = props.getStringList(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), ",",
        Collections.emptyList());
    customAvroKeyGenerator = new CustomAvroKeyGenerator(props);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return customAvroKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return customAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    validateRecordKeyFields();
    return getRecordKeyFields().size() == 1
        ? new SimpleKeyGenerator(config).getRecordKey(row)
        : new ComplexKeyGenerator(config).getRecordKey(row);
  }

  @Override
  public String getPartitionPath(Row row) {
    return getPartitionPath(Option.empty(), Option.of(row), Option.empty());
  }

  @Override
  public String getPartitionPath(InternalRow row, StructType structType) {
    return getPartitionPath(Option.empty(), Option.empty(), Option.of(Pair.of(row, structType)));
  }

  private String getPartitionPath(Option<GenericRecord> record, Option<Row> row, Option<Pair<InternalRow, StructType>> internalRowStructTypePair) {
    if (getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for partition path in cfg");
    }

    String partitionPathField;
    StringBuilder partitionPath = new StringBuilder();

    //Corresponds to no partition case
    if (getPartitionPathFields().size() == 1 && getPartitionPathFields().get(0).isEmpty()) {
      return "";
    }
    for (String field : getPartitionPathFields()) {
      String[] fieldWithType = field.split(customAvroKeyGenerator.SPLIT_REGEX);
      if (fieldWithType.length != 2) {
        throw new HoodieKeyGeneratorException("Unable to find field names for partition path in proper format");
      }

      partitionPathField = fieldWithType[0];
      CustomAvroKeyGenerator.PartitionKeyType keyType = CustomAvroKeyGenerator.PartitionKeyType.valueOf(fieldWithType[1].toUpperCase());
      switch (keyType) {
        case SIMPLE:
          if (record.isPresent()) {
            partitionPath.append(new SimpleKeyGenerator(config, partitionPathField).getPartitionPath(record.get()));
          } else if (row.isPresent()) {
            partitionPath.append(new SimpleKeyGenerator(config, partitionPathField).getPartitionPath(row.get()));
          } else {
            partitionPath.append(new SimpleKeyGenerator(config, partitionPathField).getPartitionPath(internalRowStructTypePair.get().getKey(),
                internalRowStructTypePair.get().getValue()));
          }
          break;
        case TIMESTAMP:
          try {
            if (record.isPresent()) {
              partitionPath.append(new TimestampBasedKeyGenerator(config, partitionPathField).getPartitionPath(record.get()));
            } else if (row.isPresent()) {
              partitionPath.append(new TimestampBasedKeyGenerator(config, partitionPathField).getPartitionPath(row.get()));
            } else {
              partitionPath.append(new TimestampBasedKeyGenerator(config, partitionPathField).getPartitionPath(internalRowStructTypePair.get().getKey(),
                  internalRowStructTypePair.get().getValue()));
            }
          } catch (IOException ioe) {
            throw new HoodieKeyGeneratorException("Unable to initialise TimestampBasedKeyGenerator class", ioe);
          }
          break;
        default:
          throw new HoodieKeyGeneratorException("Please provide valid PartitionKeyType with fields! You provided: " + keyType);
      }

      partitionPath.append(customAvroKeyGenerator.getDefaultPartitionPathSeparator());
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  private void validateRecordKeyFields() {
    if (getRecordKeyFields() == null || getRecordKeyFields().isEmpty()) {
      throw new HoodieKeyException("Unable to find field names for record key in cfg");
    }
  }
}

