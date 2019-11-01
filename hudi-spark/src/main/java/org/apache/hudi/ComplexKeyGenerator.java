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

import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;

/**
 * Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class ComplexKeyGenerator extends KeyGenerator {

  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  public static final String DEFAULT_RECORD_KEY_SEPARATOR = ":";
  private static final String DEFAULT_PARTITION_PATH = "default";

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(","));
    this.partitionPathFields = Arrays.asList(props
        .getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()).split(","));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyFields == null || partitionPathFields == null) {
      throw new HoodieException(
          "Unable to find field names for record key or partition path in cfg");
    }
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      recordKey.append(StringUtils.join(recordKeyField, DEFAULT_RECORD_KEY_SEPARATOR, DataSourceUtils
          .getNestedFieldValAsString(record, recordKeyField),","));
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    StringBuilder partitionPath = new StringBuilder();
    try {
      for (String partitionPathField : partitionPathFields) {
        partitionPath.append(DataSourceUtils.getNestedFieldValAsString(record, partitionPathField));
        partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
      partitionPath.deleteCharAt(partitionPath.length() - 1);
    } catch (HoodieException e) {
      partitionPath = partitionPath.append(DEFAULT_PARTITION_PATH);
    }

    return new HoodieKey(recordKey.toString(), partitionPath.toString());
  }
}
