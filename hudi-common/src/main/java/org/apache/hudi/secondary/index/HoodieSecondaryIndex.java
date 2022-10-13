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

package org.apache.hudi.secondary.index;

import org.apache.hudi.exception.HoodieSecondaryIndexException;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

public class HoodieSecondaryIndex {
  private String indexName;
  private SecondaryIndexType indexType;

  // The index fields need to be in order
  private LinkedHashMap<String, Map<String, String>> columns;
  private Map<String, String> options;

  public HoodieSecondaryIndex() {
  }

  public HoodieSecondaryIndex(
      String indexName,
      SecondaryIndexType indexType,
      LinkedHashMap<String, Map<String, String>> columns,
      Map<String, String> options) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.columns = columns;
    this.options = options;

    validate();
  }

  public String getIndexName() {
    return indexName;
  }

  public SecondaryIndexType getIndexType() {
    return indexType;
  }

  public Map<String, Map<String, String>> getColumns() {
    return columns;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public static Builder builder() {
    return new Builder();
  }

  private void validate() {
    switch (indexType) {
      case LUCENE:
        if (columns.size() != 1) {
          throw new HoodieSecondaryIndexException("Lucene index only support single column");
        }
        break;
      default:
        return;
    }
  }

  @Override
  public String toString() {
    return "HoodieIndex{"
        + "indexName='" + indexName + '\''
        + ", indexType=" + indexType
        + ", columns=" + columns
        + ", options=" + options
        + '}';
  }

  public static class Builder {
    private String indexName;
    private SecondaryIndexType indexType;
    private LinkedHashMap<String, Map<String, String>> columns;
    private Map<String, String> options;

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setIndexType(String indexType) {
      this.indexType = SecondaryIndexType.of(indexType);
      return this;
    }

    public Builder setColumns(LinkedHashMap<String, Map<String, String>> columns) {
      this.columns = columns;
      return this;
    }

    public Builder setOptions(Map<String, String> options) {
      this.options = options;
      return this;
    }

    public HoodieSecondaryIndex build() {
      return new HoodieSecondaryIndex(indexName, indexType, columns, options);
    }
  }

  public static class HoodieIndexCompactor implements Comparator<HoodieSecondaryIndex> {
    @Override
    public int compare(HoodieSecondaryIndex o1, HoodieSecondaryIndex o2) {
      return o1.indexName.compareTo(o2.indexName);
    }
  }
}
