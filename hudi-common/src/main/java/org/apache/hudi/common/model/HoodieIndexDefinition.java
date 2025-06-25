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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.DAYS_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.FORMAT_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.LENGTH_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.PATTERN_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.POSITION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.REGEX_GROUP_INDEX_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.REPLACEMENT_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.TRIM_STRING_OPTION;

/**
 * Class representing the metadata for a functional or secondary index in Hudi.
 */
@JsonIgnoreProperties(ignoreUnknown = true, value = {"sourceFieldsKey"})
public class HoodieIndexDefinition implements Serializable {

  // Name of the index
  private String indexName;

  private String indexType;

  // Expression used for the index, e.g., MONTH(ts)
  private String indexFunction;

  // Data fields the expression is derived from
  private List<String> sourceFields;

  /**
   * Source fields concatenated with dot '.'.
   *
   * <p>The field should be ignored in Ser/De.
   */
  private String sourceFieldsKey;

  // Any other configuration or properties specific to the index
  private Map<String, String> indexOptions;

  public HoodieIndexDefinition() {
  }

  HoodieIndexDefinition(String indexName, String indexType, String indexFunction, List<String> sourceFields, Map<String, String> indexOptions) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.indexFunction = nonEmpty(indexFunction) ? indexFunction : EMPTY_STRING;
    this.sourceFields = sourceFields;
    this.indexOptions = indexOptions;
  }

  public String getIndexFunction() {
    return indexFunction;
  }

  public List<String> getSourceFields() {
    return sourceFields;
  }

  public String getSourceFieldsKey() {
    if (this.sourceFieldsKey == null) {
      this.sourceFieldsKey = String.join(".", this.sourceFields);
    }
    return this.sourceFieldsKey;
  }

  public Map<String, String> getIndexOptions() {
    return indexOptions;
  }

  public String getExpressionIndexFormatOption(String defaultValue) {
    return indexOptions.getOrDefault(FORMAT_OPTION, defaultValue);
  }

  public String getExpressionIndexFormatOption() {
    return indexOptions.get(FORMAT_OPTION);
  }

  public String getExpressionIndexDaysOption() {
    return indexOptions.get(DAYS_OPTION);
  }

  public String getExpressionIndexPositionOption() {
    return indexOptions.get(POSITION_OPTION);
  }

  public String getExpressionIndexLengthOption() {
    return indexOptions.get(LENGTH_OPTION);
  }

  public String getExpressionIndexPatternOption() {
    return indexOptions.get(PATTERN_OPTION);
  }

  public String getExpressionIndexReplacementOption() {
    return indexOptions.get(REPLACEMENT_OPTION);
  }

  public String getExpressionIndexIndexOption() {
    return indexOptions.get(REGEX_GROUP_INDEX_OPTION);
  }

  public String getExpressionIndexTrimStringOption() {
    return indexOptions.get(TRIM_STRING_OPTION);
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexType() {
    return indexType;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String indexName;
    private String indexType;
    private String indexFunction;
    private List<String> sourceFields;
    private Map<String, String> indexOptions;

    public Builder() {
      this.sourceFields = new ArrayList<>();
      this.indexOptions = new HashMap<>();
    }

    public Builder withIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder withIndexType(String indexType) {
      this.indexType = indexType;
      return this;
    }

    public Builder withIndexFunction(String indexFunction) {
      this.indexFunction = indexFunction;
      return this;
    }

    public Builder withSourceFields(List<String> sourceFields) {
      this.sourceFields = sourceFields;
      return this;
    }

    public Builder withIndexOptions(Map<String, String> indexOptions) {
      this.indexOptions = indexOptions;
      return this;
    }

    public HoodieIndexDefinition build() {
      return new HoodieIndexDefinition(indexName, indexType, indexFunction, sourceFields, indexOptions);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", HoodieIndexDefinition.class.getSimpleName() + "[", "]")
        .add("indexName='" + indexName + "'")
        .add("indexType='" + indexType + "'")
        .add("indexFunction='" + indexFunction + "'")
        .add("sourceFields=" + sourceFields)
        .add("indexOptions=" + indexOptions)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HoodieIndexDefinition)) {
      return false;
    }
    HoodieIndexDefinition that = (HoodieIndexDefinition) o;
    return getIndexName().equals(that.getIndexName()) && getIndexType().equals(that.getIndexType())
        && getIndexFunction().equals(that.getIndexFunction()) && getSourceFields().equals(that.getSourceFields())
        && getIndexOptions().equals(that.getIndexOptions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getIndexName(), getIndexType(), getIndexFunction(), getSourceFields(), getIndexOptions());
  }
}
