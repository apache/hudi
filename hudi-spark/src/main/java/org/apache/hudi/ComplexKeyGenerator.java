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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;

/**
 * Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class ComplexKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";
  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  private static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  private static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  protected final List<String> recordKeyFields;

  protected final List<String> partitionPathFields;

  protected final boolean hiveStylePartitioning;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(","));
    this.partitionPathFields =
        Arrays.asList(props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()).split(","));
    this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyFields == null || partitionPathFields == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }

    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      String recordKeyValue = DataSourceUtils.getNullableNestedFieldValAsString(record, recordKeyField);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
      } else {
        recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + recordKeyFields.toString() + " cannot be entirely null or empty.");
    }

    StringBuilder partitionPath = new StringBuilder();
    for (String partitionPathField : partitionPathFields) {
      String fieldVal = DataSourceUtils.getNullableNestedFieldValAsString(record, partitionPathField);
      if (fieldVal == null || fieldVal.isEmpty()) {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + DEFAULT_PARTITION_PATH
                : DEFAULT_PARTITION_PATH);
      } else {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + fieldVal : fieldVal);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);

    return new HoodieKey(recordKey.toString(), partitionPath.toString());
  }

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }
}
