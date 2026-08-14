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

package org.apache.hudi.client.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for exception-handling behavior in {@link SparkPreCommitValidator#validate}.
 */
@ExtendWith(MockitoExtension.class)
public class TestSparkPreCommitValidator {

  @Mock
  @SuppressWarnings("rawtypes")
  private HoodieSparkTable table;

  @Mock
  private HoodieEngineContext engineContext;

  @Mock
  private HoodieWriteConfig writeConfig;

  @Mock
  @SuppressWarnings("rawtypes")
  private HoodieWriteMetadata writeMetadata;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    when(writeConfig.getTableName()).thenReturn("test-table");
    when(writeConfig.isMetricsOn()).thenReturn(false);
    when(writeMetadata.getWriteStats()).thenReturn(Option.of(Collections.emptyList()));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateSucceeds() {
    SparkPreCommitValidator<?, ?, ?, HoodieData<WriteStatus>> validator =
        new NoOpValidator(table, engineContext, writeConfig);
    assertDoesNotThrow(() -> validator.validate("001", writeMetadata, null, null),
        "validate must not throw when validateRecordsBeforeAndAfter completes normally");
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateRethrowsUnexpectedRuntimeException() {
    RuntimeException cause = new RuntimeException("disk full");
    SparkPreCommitValidator<?, ?, ?, HoodieData<WriteStatus>> validator =
        new ThrowingValidator(table, engineContext, writeConfig, cause);

    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> validator.validate("001", writeMetadata, null, null));
    assertSame(cause, ex,
        "unexpected RuntimeException must propagate as-is so the operator sees the original stack trace, "
            + "not a generic 'validation failed' message");
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateReThrowsValidationException() {
    HoodieValidationException original = new HoodieValidationException("bad data");
    SparkPreCommitValidator<?, ?, ?, HoodieData<WriteStatus>> validator =
        new ThrowingValidator(table, engineContext, writeConfig, original);

    HoodieValidationException ex = assertThrows(HoodieValidationException.class,
        () -> validator.validate("001", writeMetadata, null, null));
    assertSame(original, ex,
        "HoodieValidationException must be rethrown as-is without additional wrapping");
  }

  /** Minimal concrete validator that completes normally. */
  private static class NoOpValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {

    NoOpValidator(HoodieSparkTable<T> table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after,
                                                 Set<String> partitionsAffected) {
      // no-op — validation always passes
    }
  }

  /** Minimal concrete validator that throws a fixed exception from validateRecordsBeforeAndAfter. */
  private static class ThrowingValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {

    private final RuntimeException toThrow;

    ThrowingValidator(HoodieSparkTable<T> table, HoodieEngineContext context,
                      HoodieWriteConfig config, RuntimeException toThrow) {
      super(table, context, config);
      this.toThrow = toThrow;
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after,
                                                 Set<String> partitionsAffected) {
      throw toThrow;
    }
  }
}
