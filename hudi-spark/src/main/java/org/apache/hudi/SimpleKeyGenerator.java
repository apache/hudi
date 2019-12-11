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

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";

  protected final String recordKeyField;

  protected final String partitionPathField;

  protected final boolean hiveStylePartitioning;

  public SimpleKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyField = props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY());
    this.partitionPathField = props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY());
    this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyField == null || partitionPathField == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }

    String recordKey = DataSourceUtils.getNullableNestedFieldValAsString(record, recordKeyField);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
    }

    String partitionPath = DataSourceUtils.getNullableNestedFieldValAsString(record, partitionPathField);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    if (hiveStylePartitioning) {
      partitionPath = partitionPathField + "=" + partitionPath;
    }

    return new HoodieKey(recordKey, partitionPath);
  }
}
