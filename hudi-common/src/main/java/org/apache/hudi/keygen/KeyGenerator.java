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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.function.Function;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class KeyGenerator implements KeyGeneratorInterface {
  public static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  public static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";
  public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";
  public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";

  protected final TypedProperties config;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract HoodieKey getKey(GenericRecord record);

  /**
   * Used during bootstrap, to project out only the record key fields from bootstrap source dataset.
   *
   * @return list of field names, when concatenated make up the record key.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<String> getRecordKeyFieldNames() {
    throw new UnsupportedOperationException("Bootstrap not supported for key generator. "
        + "Please override this method in your custom key generator.");
  }

  public static String constructRecordKey(List<String> recordKeyFields, Function<String, Object> recordKeyValueFunction) {
    if (recordKeyFields.size() == 1) {
      Object recordKeyValue = recordKeyValueFunction.apply(recordKeyFields.get(0));
      if (recordKeyValue == null) {
        throw new HoodieKeyException("recordKey cannot be null");
      }
      return recordKeyValue.toString();
    }
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (int i = 0; i < recordKeyFields.size(); i++) {
      String recordKeyField = recordKeyFields.get(i);
      Object recordKeyValue = recordKeyValueFunction.apply(recordKeyField);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(NULL_RECORDKEY_PLACEHOLDER);
      } else if (recordKeyValue.toString().isEmpty()) {
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(EMPTY_RECORDKEY_PLACEHOLDER);
      } else {
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(recordKeyValue);
        keyIsNullEmpty = false;
      }
      if (i != recordKeyFields.size() - 1) {
        recordKey.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + recordKeyFields + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }
}
