/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sql;

import java.util.Locale;

/**
 * Insert mode for insert into pk-table.
 */
public enum InsertMode {
  /**
   * In upsert mode for insert into, duplicate record on primary key
   * will be updated.This is the default insert mode for pk-table.
   */
  UPSERT("upsert"),
  /**
   * In strict mode for insert into, we do the pk uniqueness guarantee
   * for COW pk-table.
   * For MOR pk-table, it has the same behavior with "upsert" mode.
   */
  STRICT("strict"),
  /**
   * In non-strict mode for insert into, we use insert operation
   * to write data which allow writing the duplicate record.
   */
  NON_STRICT("non-strict")
  ;

  private String value;

  InsertMode(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static InsertMode of(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "upsert":
        return UPSERT;
      case "strict":
        return STRICT;
      case "non-strict":
        return NON_STRICT;
      default:
        throw new AssertionError("UnSupport Insert Mode: " + value);
    }
  }
}
