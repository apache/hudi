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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class representing the Column Family definition.
 */
public class ColumnFamilyDefinition implements Serializable {

  private static final String COLUMNS_DELIMITER = ",";
  private static final String PRE_COMBINE_DELIMITER = ";";

  // Name of the family
  private final String name;

  // Columns this family consists of
  private final List<String> columns;

  // preCombine column of this family
  private final String preCombine;

  private ColumnFamilyDefinition(String name, List<String> columns, String preCombine) {
    this.name = name;
    this.columns = columns;
    this.preCombine = preCombine;
    validate();
  }

  public static ColumnFamilyDefinition fromConfig(String name, String configValue) {
    if (StringUtils.isNullOrEmpty(configValue)) {
      throw new HoodieValidationException("Column family must contain more than 1 column");
    } else {
      String[] fieldStrings = configValue.split(PRE_COMBINE_DELIMITER);
      String preCombine = null;
      if (fieldStrings.length > 2) {
        throw new HoodieValidationException("Only one semicolon delimiter allowed to separate precombine column");
      } else if (fieldStrings.length == 2) {
        preCombine = fieldStrings[1].trim();
      }
      return new ColumnFamilyDefinition(
          name,
          Arrays.stream(fieldStrings[0].split(COLUMNS_DELIMITER)).map(String::trim).collect(Collectors.toList()),
          preCombine
      );
    }
  }

  public String toConfigValue() {
    return String.join(COLUMNS_DELIMITER, columns)
        + (!StringUtils.isNullOrEmpty(preCombine) ? PRE_COMBINE_DELIMITER + preCombine : "");
  }

  private void validate() {
    if (StringUtils.isNullOrEmpty(name)) {
      throw new HoodieValidationException("Column family name must not be empty");
    }
    if (columns == null || columns.size() < 2) {
      throw new HoodieValidationException("Column family must contain more than 1 column");
    }
    columns.forEach(column -> {
      if (StringUtils.isNullOrEmpty(column)) {
        throw new HoodieValidationException("Column name must not be empty");
      }
    });
    if (columns.size() > new HashSet<>(columns).size()) {
      throw new HoodieValidationException("Family's columns must not contain duplicates");
    }
    if (!StringUtils.isNullOrEmpty(preCombine) && !columns.contains(preCombine)) {
      throw new HoodieValidationException("Family's columns must contain preCombine column: " + preCombine);
    }
  }

  public String getName() {
    return name;
  }

  public List<String> getColumns() {
    return columns;
  }

  public String getPreCombine() {
    return preCombine;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieColumnFamilyDefinition {");
    sb.append("name='").append(name);
    sb.append("', columns=").append(columns);
    sb.append(", preCombine='").append(preCombine).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
