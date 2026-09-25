/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.EnumFieldDescription;

import java.util.Arrays;
import java.util.Locale;

/**
 * How {@code _hoodie_record_key} is encoded for a {@code ComplexKeyGenerator} configured with a
 * single record key field.
 *
 * <p>Persisted in {@code hoodie.properties} as {@code hoodie.table.complex.keygenerator.encoding}, on creation for
 * new tables and by the first write or upgrade that finds it missing on an existing table, so that writers and
 * readers know the format the table actually carries instead of assuming it from the table version.
 */
public enum ComplexKeyGenEncoding {
  @EnumFieldDescription("Record key is stored as `<field_name>:<field_value>`. Written by Hudi 0.14.0 and older, "
      + "by 1.1.0 and later, and the encoding of every newly created table.")
  FIELD_PREFIXED,

  @EnumFieldDescription("Record key is stored as the bare `<field_value>`. Written by Hudi 0.14.1, 0.15.0, 1.0.0, "
      + "1.0.1 and 1.0.2, and kept by such a table across later upgrades.")
  VALUE_ONLY;

  /**
   * @param useNewEncoding the value of {@code hoodie.write.complex.keygen.new.encoding}
   *                       ({@code true} means the bare value).
   */
  public static ComplexKeyGenEncoding fromUseNewEncoding(boolean useNewEncoding) {
    return useNewEncoding ? VALUE_ONLY : FIELD_PREFIXED;
  }

  /**
   * Parses the persisted string form, tolerating case and surrounding whitespace.
   */
  public static ComplexKeyGenEncoding fromString(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("hoodie.table.complex.keygenerator.encoding is set but empty; remove the line "
          + "or set it to one of " + Arrays.toString(values()));
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      // Reached on every read and write of the table, including on executors, so name the property and the
      // legal values rather than letting "No enum constant" surface with no context.
      throw new IllegalArgumentException("Unrecognized hoodie.table.complex.keygenerator.encoding '" + value
          + "'; expected one of " + Arrays.toString(values()), e);
    }
  }

  /**
   * @return whether the field name is prepended to the record key value.
   */
  public boolean encodesFieldName() {
    return this == FIELD_PREFIXED;
  }
}
