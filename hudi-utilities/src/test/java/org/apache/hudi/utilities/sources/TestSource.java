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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Source#isAllowSourcePersistRdd()}.
 *
 * <p>The source-RDD persist controlled by
 * {@code hoodie.errortable.source.rdd.persist} is bypassed when the writer is
 * configured with a metadata-table-backed record index, since
 * {@code SparkMetadataTableRecordIndex#tagLocation} already persists the
 * records RDD at {@code MEMORY_AND_DISK_SER}. Double-caching has caused
 * executor OOMKills on RLI-indexed tables.
 */
public class TestSource {

  @Test
  public void isAllowSourcePersistRddReturnsFalseWhenPersistFlagIsDisabled() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), "false");
    props.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), HoodieIndex.IndexType.BLOOM.name());

    assertFalse(new NoopSource(props).isAllowSourcePersistRdd(),
        "persist flag off must always disable source persist");
  }

  @Test
  public void isAllowSourcePersistRddReturnsTrueWhenIndexTypeIsAbsent() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), "true");

    assertTrue(new NoopSource(props).isAllowSourcePersistRdd(),
        "absent index type must not bypass persist (engine-level default applies elsewhere)");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class,
      names = {"RECORD_INDEX", "PARTITIONED_RECORD_INDEX"})
  public void isAllowSourcePersistRddReturnsFalseForRecordIndexTypes(HoodieIndex.IndexType indexType) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), "true");
    props.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), indexType.name());

    assertFalse(new NoopSource(props).isAllowSourcePersistRdd(),
        "RLI index types must bypass source persist to avoid double-caching");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class,
      names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE", "BUCKET", "HBASE", "INMEMORY"})
  public void isAllowSourcePersistRddReturnsTrueForNonRecordIndexTypes(HoodieIndex.IndexType indexType) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), "true");
    props.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), indexType.name());

    assertTrue(new NoopSource(props).isAllowSourcePersistRdd(),
        "non-RLI index types must preserve existing source-persist behavior");
  }

  @Test
  public void isAllowSourcePersistRddIsCaseInsensitiveOnIndexType() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), "true");
    props.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), "record_index");

    assertFalse(new NoopSource(props).isAllowSourcePersistRdd(),
        "lower-case index type must still be recognized as RLI");
  }

  /**
   * Minimal {@link Source} subclass for exercising the persist-gating logic without standing
   * up a real Spark context. The constructor only stores the inputs and reads the persist
   * and storage-level configs, so passing {@code null} for the Spark fields is safe — they
   * are never touched by {@link Source#isAllowSourcePersistRdd()}.
   */
  private static final class NoopSource extends Source<Object> {
    NoopSource(TypedProperties props) {
      super(props, null, null, SourceType.AVRO, new TestStreamContext());
    }

    @Override
    protected InputBatch<Object> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
      throw new UnsupportedOperationException("not used in this test");
    }
  }

  private static final class TestStreamContext implements StreamContext {
    @Override
    public SchemaProvider getSchemaProvider() {
      return null;
    }

    @Override
    public Option<SourceProfileSupplier> getSourceProfileSupplier() {
      return Option.empty();
    }
  }
}
