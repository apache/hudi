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

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieException;
import org.apache.avro.generic.GenericRecord;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as
 * configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";

  protected final String recordKeyField;

  protected final String partitionPathField;

  public SimpleKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyField = props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY());
    this.partitionPathField = props
        .getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY());
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyField == null || partitionPathField == null) {
      throw new HoodieException(
          "Unable to find field names for record key or partition path in cfg");
    }

    String recordKey = DataSourceUtils.getNestedFieldValAsString(record, recordKeyField);
    String partitionPath;
    try {
      partitionPath = DataSourceUtils.getNestedFieldValAsString(record, partitionPathField);
    } catch (HoodieException e) {
      // if field is not found, lump it into default partition
      partitionPath = DEFAULT_PARTITION_PATH;
    }

    return new HoodieKey(recordKey, partitionPath);
  }
}
