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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class HoodieIndexDefinition implements Serializable {

  // Index name is composed of 2 parts - MDT partition path prefix + user-specified index name.
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
  @JsonIgnore
  @ToString.Exclude
  @EqualsAndHashCode.Exclude
  private String sourceFieldsKey;

  // Any other configuration or properties specific to the index
  private Map<String, String> indexOptions;

  // Version of the index
  private HoodieIndexVersion version;

  private HoodieIndexDefinition(
      String indexName,
      String indexType,
      String indexFunction,
      List<String> sourceFields,
      Map<String, String> indexOptions,
      HoodieIndexVersion version) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.indexFunction = nonEmpty(indexFunction) ? indexFunction : EMPTY_STRING;
    this.sourceFields = sourceFields;
    this.indexOptions = indexOptions;
    this.version = version;
  }

  public String getSourceFieldsKey() {
    if (this.sourceFieldsKey == null) {
      this.sourceFieldsKey = String.join(".", this.sourceFields);
    }
    return this.sourceFieldsKey;
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

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Create a new Builder pre-populated with values from this instance.
   */
  public Builder toBuilder() {
    return new Builder()
        .withIndexName(this.indexName)
        .withIndexType(this.indexType)
        .withIndexFunction(this.indexFunction)
        .withSourceFields(new ArrayList<>(this.sourceFields))
        .withIndexOptions(new HashMap<>(this.indexOptions))
        .withVersion(this.version);
  }

  /**
   * Builder for {@link HoodieIndexDefinition}.
   */
  @NoArgsConstructor
  public static class Builder {

    private String indexName;
    private String indexType;
    private String indexFunction;
    private List<String> sourceFields = new ArrayList<>();
    private Map<String, String> indexOptions = new HashMap<>();
    private HoodieIndexVersion version;

    public Builder withIndexName(String indexName) {
      ValidationUtils.checkArgument(MetadataPartitionType.fromPartitionPath(indexName) != null,
          "Invalid index name");
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

    public Builder withVersion(HoodieIndexVersion version) {
      this.version = version;
      return this;
    }

    public HoodieIndexDefinition build() {
      ValidationUtils.checkArgument(indexName != null, "Could not build index definition with a null index name");
      ValidationUtils.checkArgument(indexType != null, "Could not build index definition with a null index type");
      return new HoodieIndexDefinition(
          indexName,
          indexType,
          indexFunction,
          sourceFields,
          indexOptions,
          version
      );
    }
  }
}
