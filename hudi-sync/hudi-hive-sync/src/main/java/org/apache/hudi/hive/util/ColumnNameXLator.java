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

package org.apache.hudi.hive.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ColumnNameXLator {

  private static final Map<String, String> X_FORM_MAP = new HashMap<>();

  public static String translateNestedColumn(String colName) {
    Map.Entry<String,String> entry;
    for (Iterator<Map.Entry<String, String>> ic = X_FORM_MAP.entrySet().iterator(); ic.hasNext(); colName =
        colName.replaceAll(entry.getKey(), entry.getValue())) {
      entry = ic.next();
    }

    return colName;
  }

  public static String translateColumn(String colName) {
    return colName;
  }

  public static String translate(String colName, boolean nestedColumn) {
    return !nestedColumn ? translateColumn(colName) : translateNestedColumn(colName);
  }

  static {
    X_FORM_MAP.put("\\$", "_dollar_");
  }
}
