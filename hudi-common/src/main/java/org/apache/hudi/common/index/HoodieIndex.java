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

package org.apache.hudi.common.index;

import java.util.Arrays;
import java.util.Map;

public class HoodieIndex {
  private String indexName;
  private String[] colNames;
  private HoodieIndexType indexType;
  private Map<String, Map<String, String>> colOptions;
  private Map<String, String> options;

  public HoodieIndex() {
  }

  public HoodieIndex(
      String indexName,
      String[] colNames,
      HoodieIndexType indexType,
      Map<String, Map<String, String>> colOptions,
      Map<String, String> options) {
    this.indexName = indexName;
    this.colNames = colNames;
    this.indexType = indexType;
    this.colOptions = colOptions;
    this.options = options;
  }

  public String getIndexName() {
    return indexName;
  }

  public String[] getColNames() {
    return colNames;
  }

  public HoodieIndexType getIndexType() {
    return indexType;
  }

  public Map<String, Map<String, String>> getColOptions() {
    return colOptions;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "HoodieIndex{"
        + "indexName='" + indexName + '\''
        + ", colNames='" + Arrays.toString(colNames) + '\''
        + ", indexType=" + indexType
        + ", colOptions=" + colOptions
        + ", options=" + options
        + '}';
  }

  public static class Builder {
    private String indexName;
    private String[] colNames;
    private HoodieIndexType indexType;
    private Map<String, Map<String, String>> colOptions;
    private Map<String, String> options;

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setColNames(String[] colNames) {
      this.colNames = colNames;
      return this;
    }

    public Builder setIndexType(String indexType) {
      this.indexType = HoodieIndexType.of(indexType);
      return this;
    }

    public Builder setColOptions(Map<String, Map<String, String>> colOptions) {
      this.colOptions = colOptions;
      return this;
    }

    public Builder setOptions(Map<String, String> options) {
      this.options = options;
      return this;
    }

    public HoodieIndex build() {
      return new HoodieIndex(indexName, colNames, indexType, colOptions, options);
    }
  }
}
