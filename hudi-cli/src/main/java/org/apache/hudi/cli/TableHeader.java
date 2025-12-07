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

package org.apache.hudi.cli;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Header for the table to be rendered.
 */
public class TableHeader {

  // List of fields (columns)
  @Getter
  private final List<String> fieldNames = new ArrayList<>();

  /**
   * Add a field (column) to table.
   *
   * @param fieldName field Name
   */
  public TableHeader addTableHeaderField(String fieldName) {
    fieldNames.add(fieldName);
    return this;
  }

  /**
   * Add fields from another {@link TableHeader} instance.
   *
   * @param tableHeader {@link TableHeader} instance.
   */
  public TableHeader addTableHeaderFields(TableHeader tableHeader) {
    fieldNames.addAll(tableHeader.getFieldNames());
    return this;
  }

  /**
   * Index of the field in the table.
   *
   * @param fieldName Field Name
   */
  public int indexOf(String fieldName) {
    return fieldNames.indexOf(fieldName);
  }

  /**
   * Lookup field by offset.
   */
  public String get(int index) {
    return fieldNames.get(index);
  }

  /**
   * Get number of fields in the table.
   */
  public int getNumFields() {
    return fieldNames.size();
  }

  public boolean containsField(String fieldName) {
    return fieldNames.contains(fieldName);
  }
}
