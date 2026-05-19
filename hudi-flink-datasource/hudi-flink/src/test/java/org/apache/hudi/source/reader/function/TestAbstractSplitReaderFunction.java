/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.utils.TestConfigurations;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link AbstractSplitReaderFunction}.
 */
public class TestAbstractSplitReaderFunction {

  @TempDir
  File tempDir;

  private Configuration conf;
  private InternalSchemaManager mockInternalSchemaManager;

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    mockInternalSchemaManager = mock(InternalSchemaManager.class);
  }

  // -----------------------------------------------------------------------
  //  Minimal concrete subclass
  // -----------------------------------------------------------------------

  /**
   * Minimal concrete implementation that exposes the protected helper methods
   * ({@code getWriteConfig()} / {@code getHadoopConf()}) for testing, and leaves
   * {@code read()} / {@code close()} as no-ops.
   */
  private static class MinimalSplitReaderFunction extends AbstractSplitReaderFunction {

    MinimalSplitReaderFunction(
        Configuration conf,
        List<ExpressionPredicates.Predicate> predicates,
        InternalSchemaManager internalSchemaManager,
        boolean emitDelete) {
      super(conf, predicates, internalSchemaManager, emitDelete);
    }

    @Override
    public RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> read(HoodieSourceSplit split) {
      return null;
    }

    @Override
    public void close() throws Exception {
    }

    HoodieWriteConfig writeConfigForTest() {
      return getWriteConfig();
    }

    org.apache.hadoop.conf.Configuration hadoopConfForTest() {
      return getHadoopConf();
    }
  }

  private MinimalSplitReaderFunction create(boolean emitDelete, List<ExpressionPredicates.Predicate> predicates) {
    return new MinimalSplitReaderFunction(conf, predicates, mockInternalSchemaManager, emitDelete);
  }

  // -----------------------------------------------------------------------
  //  Constructor — field storage
  // -----------------------------------------------------------------------

  @Test
  public void testConstructorStoresConf() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    assertSame(conf, fn.conf, "conf field must be the exact reference passed to the constructor");
  }

  @Test
  public void testConstructorStoresInternalSchemaManager() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    assertSame(mockInternalSchemaManager, fn.internalSchemaManager,
        "internalSchemaManager field must be the exact reference passed to the constructor");
  }

  @Test
  public void testConstructorStoresEmptyPredicatesList() {
    List<ExpressionPredicates.Predicate> predicates = Collections.emptyList();
    MinimalSplitReaderFunction fn = create(false, predicates);
    assertSame(predicates, fn.predicates,
        "predicates field must be the exact list reference passed to the constructor");
    assertTrue(fn.predicates.isEmpty());
  }

  @Test
  public void testConstructorStoresNonEmptyPredicatesList() {
    ExpressionPredicates.Predicate predicate = ExpressionPredicates.NotEquals.getInstance()
        .bindFieldReference(new FieldReferenceExpression(
            "age", new AtomicDataType(new VarCharType(true, 10)), 0, 0))
        .bindValueLiteral(new ValueLiteralExpression("18"));
    List<ExpressionPredicates.Predicate> predicates = Collections.singletonList(predicate);

    MinimalSplitReaderFunction fn = create(false, predicates);

    assertSame(predicates, fn.predicates,
        "predicates field must be the exact list reference passed to the constructor");
    assertEquals(1, fn.predicates.size());
    assertSame(predicate, fn.predicates.get(0));
  }

  @Test
  public void testConstructorStoresEmitDeleteFalse() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    assertFalse(fn.emitDelete, "emitDelete must be false when false is passed to the constructor");
  }

  @Test
  public void testConstructorStoresEmitDeleteTrue() {
    MinimalSplitReaderFunction fn = create(true, Collections.emptyList());
    assertTrue(fn.emitDelete, "emitDelete must be true when true is passed to the constructor");
  }

  // -----------------------------------------------------------------------
  //  getHadoopConf() — lazy singleton
  // -----------------------------------------------------------------------

  @Test
  public void testGetHadoopConfReturnsNonNull() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    assertNotNull(fn.hadoopConfForTest());
  }

  @Test
  public void testGetHadoopConfIsSingleton() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    org.apache.hadoop.conf.Configuration first = fn.hadoopConfForTest();
    org.apache.hadoop.conf.Configuration second = fn.hadoopConfForTest();
    assertSame(first, second,
        "getHadoopConf() must return the same instance on every call (lazy singleton)");
  }

  // -----------------------------------------------------------------------
  //  getWriteConfig() — lazy singleton
  // -----------------------------------------------------------------------

  @Test
  public void testGetWriteConfigReturnsNonNull() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    assertNotNull(fn.writeConfigForTest());
  }

  @Test
  public void testGetWriteConfigIsSingleton() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    HoodieWriteConfig first = fn.writeConfigForTest();
    HoodieWriteConfig second = fn.writeConfigForTest();
    assertSame(first, second,
        "getWriteConfig() must return the same instance on every call (lazy singleton)");
  }

  @Test
  public void testGetWriteConfigBasePathMatchesConf() {
    MinimalSplitReaderFunction fn = create(false, Collections.emptyList());
    HoodieWriteConfig writeConfig = fn.writeConfigForTest();
    // The write config must derive its base path from FlinkOptions.PATH in the conf.
    assertTrue(writeConfig.getBasePath().contains(tempDir.getName()),
        "Write config base path must reflect the table path set in the flink configuration");
  }

  // -----------------------------------------------------------------------
  //  Independence of lazy singletons across distinct instances
  // -----------------------------------------------------------------------

  @Test
  public void testTwoInstancesHaveStableIndependentHadoopConf() {
    MinimalSplitReaderFunction fn1 = create(false, Collections.emptyList());
    MinimalSplitReaderFunction fn2 = create(false, Collections.emptyList());

    org.apache.hadoop.conf.Configuration hadoopConf1 = fn1.hadoopConfForTest();
    org.apache.hadoop.conf.Configuration hadoopConf2 = fn2.hadoopConfForTest();

    assertNotNull(hadoopConf1);
    assertNotNull(hadoopConf2);
    // Each instance must keep returning its own stable singleton.
    assertSame(hadoopConf1, fn1.hadoopConfForTest(),
        "fn1's hadoopConf must remain the same singleton across calls");
    assertSame(hadoopConf2, fn2.hadoopConfForTest(),
        "fn2's hadoopConf must remain the same singleton across calls");
  }

  @Test
  public void testTwoInstancesHaveStableIndependentWriteConfig() {
    MinimalSplitReaderFunction fn1 = create(false, Collections.emptyList());
    MinimalSplitReaderFunction fn2 = create(false, Collections.emptyList());

    HoodieWriteConfig wc1 = fn1.writeConfigForTest();
    HoodieWriteConfig wc2 = fn2.writeConfigForTest();

    assertNotNull(wc1);
    assertNotNull(wc2);
    // Each instance must keep returning its own stable singleton.
    assertSame(wc1, fn1.writeConfigForTest(),
        "fn1's writeConfig must remain the same singleton across calls");
    assertSame(wc2, fn2.writeConfigForTest(),
        "fn2's writeConfig must remain the same singleton across calls");
  }
}
