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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.SourceContracts;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;

/** A source without a source contract is built with the stream context its caller passed, as upstream builds it. */
@Tag("functional")
class TestStreamSyncStockSourceContext extends UtilitiesTestBase {

  @TempDir
  Path tempDir;

  @BeforeAll
  static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  void resetSource() {
    ContextRecordingSource.SEEN.set(null);
  }

  private HoodieStreamer.Config config() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tempDir.resolve("table").toUri().toString();
    cfg.targetTableName = "stock_source_context";
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.sourceClassName = ContextRecordingSource.class.getName();
    Triple<RecordMergeMode, String, String> merging = HoodieTableConfig.inferMergingConfigsForWrites(
        null, cfg.payloadClassName, cfg.recordMergeStrategyId, null, HoodieTableVersion.current());
    cfg.recordMergeMode = merging.getLeft();
    cfg.payloadClassName = merging.getMiddle();
    cfg.recordMergeStrategyId = merging.getRight();
    return cfg;
  }

  @Test
  void sourceWithoutContractReceivesTheCallersStreamContext() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    CallerStreamContext callerContext = new CallerStreamContext();

    try (StreamSync ignored = new StreamSync(config(), sparkSession, props, new HoodieSparkEngineContext(jsc), fs,
        jsc.hadoopConfiguration(), client -> true, callerContext)) {
      assertSame(callerContext, ContextRecordingSource.SEEN.get(), "a stock source is built with its caller's own context");
    }
  }

  /** A caller's own stream context type, which a copy into the default context would lose. */
  private static final class CallerStreamContext implements StreamContext {
    @Override
    public SchemaProvider getSchemaProvider() {
      return null;
    }

    @Override
    public Option<SourceProfileSupplier> getSourceProfileSupplier() {
      return Option.empty();
    }
  }

  /** A plain row source (no source contract) that records the stream context it is built with. */
  public static class ContextRecordingSource extends RowSource {
    static final AtomicReference<StreamContext> SEEN = new AtomicReference<>();

    public ContextRecordingSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                                  StreamContext streamContext) {
      super(props, sparkContext, sparkSession, streamContext);
      SEEN.set(streamContext);
      // a source without a contract declares none
      assertSame(SourceContracts.NONE, streamContext.getSourceContract());
    }

    @Override
    protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
      return Pair.of(Option.empty(), null);
    }
  }
}
