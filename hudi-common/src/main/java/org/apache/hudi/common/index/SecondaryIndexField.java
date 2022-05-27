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

package org.apache.hudi.common.index;

public class SecondaryIndexField {
  private String indexName;
  private String indexColumn;
  private SecondaryIndexType indexType;

  public SecondaryIndexField() {
  }

  public SecondaryIndexField(String indexName, String indexColumn, SecondaryIndexType indexType) {
    this.indexName = indexName;
    this.indexColumn = indexColumn;
    this.indexType = indexType;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexColumn() {
    return indexColumn;
  }

  public SecondaryIndexType getIndexType() {
    return indexType;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "SecondaryIndexField{"
        + "indexName='" + indexName + '\''
        + ", indexColumn='" + indexColumn + '\''
        + ", indexType=" + indexType
        + '}';
  }

  public static class Builder {
    private String indexName;
    private String indexColumn;
    private SecondaryIndexType indexType;

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setIndexColumn(String indexColumn) {
      this.indexColumn = indexColumn;
      return this;
    }

    public Builder setIndexType(String indexType) {
      this.indexType = SecondaryIndexType.of(indexType);
      return this;
    }

    public SecondaryIndexField build() {
      return new SecondaryIndexField(indexName, indexColumn, indexType);
    }
  }
}
