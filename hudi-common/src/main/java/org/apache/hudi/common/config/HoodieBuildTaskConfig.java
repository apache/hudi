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

package org.apache.hudi.common.config;

import org.apache.hudi.secondary.index.SecondaryIndexType;

import org.apache.avro.Schema;

import java.util.LinkedList;
import java.util.Properties;

public class HoodieBuildTaskConfig extends HoodieConfig {

  public static final ConfigProperty<Double> LUCENE_INDEX_RAM_BUFFER_SIZE_MB = ConfigProperty
      .key("hoodie.build.task.lucene.ram.buffer.size.mb")
      .defaultValue(32.0)
      .withDocumentation("Ram buffer size from build lucene index");

  public static final ConfigProperty<Integer> LUCENE_INDEX_MERGE_FACTOR = ConfigProperty
      .key("hoodie.build.task.lucene.merge.factor")
      .defaultValue(10)
      .withDocumentation("Determines how often segment indices are merged by addDocument()");

  public static final ConfigProperty<Double> LUCENE_INDEX_MAX_MERGE_MB = ConfigProperty
      .key("hoodie.build.task.lucene.max.merge.mb")
      .defaultValue(102400.0)
      .withDocumentation("Determines the largest segment (measured by total byte size of the segment's files, in MB) that may be merged with other segments");

  public static final ConfigProperty<Boolean> LUCENE_INDEX_LOG_ENABLED = ConfigProperty
      .key("hoodie.build.task.lucene.log.enabled")
      .defaultValue(true)
      .withDocumentation("Whether to log information about merges, deletes and a message when maxFieldLength is reached");

  public static final ConfigProperty<Integer> BUILD_BATCH_ADD_SIZE = ConfigProperty
      .key("hoodie.build.batch.add.size")
      .defaultValue(1000)
      .withDocumentation("Batch size when add records to index builder");


  private String indexSaveDir;
  private SecondaryIndexType indexType;
  private LinkedList<Schema.Field> indexFields;

  public HoodieBuildTaskConfig(String indexSaveDir,
                               SecondaryIndexType indexType,
                               LinkedList<Schema.Field> indexFields) {
    this.indexSaveDir = indexSaveDir;
    this.indexType = indexType;
    this.indexFields = indexFields;
  }

  public HoodieBuildTaskConfig(Properties props) {
    super(props);
  }

  public double getLuceneIndexRamBufferSizeMB() {
    return getDoubleOrDefault(LUCENE_INDEX_RAM_BUFFER_SIZE_MB);
  }

  public int getLuceneIndexMergeFactor() {
    return getIntOrDefault(LUCENE_INDEX_MERGE_FACTOR);
  }

  public double getLuceneIndexMaxMergeMB() {
    return getDoubleOrDefault(LUCENE_INDEX_MAX_MERGE_MB);
  }

  public boolean isLuceneIndexLogEnabled() {
    return getBooleanOrDefault(LUCENE_INDEX_LOG_ENABLED);
  }

  public String getIndexSaveDir() {
    return indexSaveDir;
  }

  public SecondaryIndexType getIndexType() {
    return indexType;
  }

  public LinkedList<Schema.Field> getIndexFields() {
    return indexFields;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String indexSaveDir;
    private SecondaryIndexType indexType;
    private LinkedList<Schema.Field> indexFields;

    public Builder setIndexSaveDir(String indexSaveDir) {
      this.indexSaveDir = indexSaveDir;
      return this;
    }

    public Builder setIndexType(SecondaryIndexType indexType) {
      this.indexType = indexType;
      return this;
    }

    public Builder setIndexFields(LinkedList<Schema.Field> indexFields) {
      this.indexFields = indexFields;
      return this;
    }

    public HoodieBuildTaskConfig build() {
      return new HoodieBuildTaskConfig(indexSaveDir, indexType, indexFields);
    }
  }
}
