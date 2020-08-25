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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;

public class KeyGenUtils {

  protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  protected static final String DEFAULT_PARTITION_PATH = "default";
  protected static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  public static String getRecordKey(GenericRecord record, List<String> recordKeyFields) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      String recordKeyValue = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true);
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
    return recordKey.toString();
  }

  public static String getRecordPartitionPath(GenericRecord record, List<String> partitionPathFields,
      boolean hiveStylePartitioning, boolean encodePartitionPath) {
    if (partitionPathFields.isEmpty()) {
      return "";
    }

    StringBuilder partitionPath = new StringBuilder();
    for (String partitionPathField : partitionPathFields) {
      String fieldVal = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true);
      if (fieldVal == null || fieldVal.isEmpty()) {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + DEFAULT_PARTITION_PATH
            : DEFAULT_PARTITION_PATH);
      } else {
        if (encodePartitionPath) {
          try {
            fieldVal = URLEncoder.encode(fieldVal, StandardCharsets.UTF_8.toString());
          } catch (UnsupportedEncodingException uoe) {
            throw new HoodieException(uoe.getMessage(), uoe);
          }
        }
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + fieldVal : fieldVal);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  public static String getRecordKey(GenericRecord record, String recordKeyField) {
    String recordKey = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
    }
    return recordKey;
  }

  public static String getPartitionPath(GenericRecord record, String partitionPathField,
      boolean hiveStylePartitioning, boolean encodePartitionPath) {
    String partitionPath = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    if (encodePartitionPath) {
      try {
        partitionPath = URLEncoder.encode(partitionPath, StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException uoe) {
        throw new HoodieException(uoe.getMessage(), uoe);
      }
    }
    if (hiveStylePartitioning) {
      partitionPath = partitionPathField + "=" + partitionPath;
    }
    return partitionPath;
  }
}