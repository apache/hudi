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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Key generator for deletes using global indices. Global index deletes do not require partition value
 * so this key generator avoids using partition value for generating HoodieKey.
 */
public class GlobalDeleteKeyGenerator extends KeyGenerator {

  private static final String EMPTY_PARTITION = "";
  private static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  private static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  protected final List<String> recordKeyFields;

  public GlobalDeleteKeyGenerator(TypedProperties config) {
    super(config);
    this.recordKeyFields = Arrays.stream(config.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(",")).map(String::trim).collect(Collectors.toList());
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    return new HoodieKey(getRecordKey(record), EMPTY_PARTITION);
  }

  String getRecordKey(GenericRecord record) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      String recordKeyValue = DataSourceUtils.getNestedFieldValAsString(record, recordKeyField, true);
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
}
