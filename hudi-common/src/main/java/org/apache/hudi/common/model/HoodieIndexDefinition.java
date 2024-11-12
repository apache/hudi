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
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX;

/**
 * Class representing the metadata for a functional or secondary index in Hudi.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieIndexDefinition implements Serializable {

  // Name of the index
  private String indexName;

  private String indexType;

  // Expression used for the index, e.g., MONTH(ts)
  private String indexFunction;

  // Data fields the expression is derived from
  private List<String> sourceFields;

  // Any other configuration or properties specific to the index
  private Map<String, String> indexOptions;

  public HoodieIndexDefinition() {
  }

  public HoodieIndexDefinition(String indexName, String indexType, String indexFunction, List<String> sourceFields,
                               Map<String, String> indexOptions) {
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

  public Map<String, String> getIndexOptions() {
    return indexOptions;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexType() {
    return indexType;
  }

  public boolean isFunctionalIndex() {
    return indexName.startsWith(PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX);
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
}
