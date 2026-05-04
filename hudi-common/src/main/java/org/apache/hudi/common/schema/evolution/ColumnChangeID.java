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

package org.apache.hudi.common.schema.evolution;

import java.util.Locale;

/**
 * The kind of column-level schema change a DDL operation represents.
 */
public enum ColumnChangeID {
  ADD, UPDATE, DELETE, PROPERTY_CHANGE, REPLACE;

  public static ColumnChangeID fromValue(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "add":
        return ADD;
      case "change":
        return UPDATE;
      case "delete":
        return DELETE;
      case "property":
        return PROPERTY_CHANGE;
      case "replace":
        return REPLACE;
      default:
        throw new IllegalArgumentException("Invalid value of Type.");
    }
  }

}
