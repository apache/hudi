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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";

  protected final boolean hiveStylePartitioning;

  public SimpleKeyGenerator(TypedProperties props) {
    super(props);
    this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()));
    this.partitionPathFields = Arrays.asList(props
        .getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyFields.size() < 1 || partitionPathFields.size() < 1) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }

    String recordKey = DataSourceUtils.getNestedFieldValAsString(record, recordKeyFields.get(0), true);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyFields + "\" "
          + "cannot be null or empty.");
    }

    String partitionPath = DataSourceUtils.getNestedFieldValAsString(record, partitionPathFields.get(0), true);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    if (hiveStylePartitioning) {
      partitionPath = partitionPathFields.get(0) + "=" + partitionPath;
    }

    return new HoodieKey(recordKey, partitionPath);
  }
}
