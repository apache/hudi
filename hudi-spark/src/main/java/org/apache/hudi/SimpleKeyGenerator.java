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
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";

  public SimpleKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()));
    this.partitionPathFields = Arrays.asList(props
        .getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    return new HoodieKey(getRecordKey(record), getPartitionPath(record));
  }

  private String getRecordKey(GenericRecord record) {
    if (recordKeyFields == null || recordKeyFields.size() < 1) {
      throw new HoodieException(
          "Unable to find field names for record key in cfg");
    }
    return DataSourceUtils.getNestedFieldValAsString(record, recordKeyFields.get(0));
  }

  private String getPartitionPath(GenericRecord record) {
    if (partitionPathFields == null || partitionPathFields.size() < 1) {
      throw new HoodieException(
          "Unable to find field names for partition path in cfg");
    }
    String partitionPath;
    try {
      partitionPath = DataSourceUtils.getNestedFieldValAsString(record, partitionPathFields.get(0));
    } catch (HoodieException e) {
      // if field is not found, lump it into default partition
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    return partitionPath;
  }
}
