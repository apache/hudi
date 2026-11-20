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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.utilities.exception.HoodieStreamerWriteException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.streamer.validator.SparkWriteErrorValidator;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestStreamSyncRollbackFailure {
  @TempDir
  Path tempDir;

  @ParameterizedTest
  @CsvSource({"write errors,false", "write errors,true", "Error table commit failed,false", "Error table commit failed,true",
      "Pre-commit validators failed,false", "Pre-commit validators failed,true"})
  void originalFailureRemainsPrimaryWhenRollbackFails(String expectedFailure, boolean rollbackFails) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(tempDir.toString());
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tempDir.toString();
    cfg.tableType = "COPY_ON_WRITE";
    cfg.operation = WriteOperationType.INSERT;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.checkpoint.force.skip", "true");
    HoodieSparkEngineContext context = mock(HoodieSparkEngineContext.class);
    Option<BaseErrorTableWriter> errorTable = expectedFailure.startsWith("Error table") ? Option.of(mock(BaseErrorTableWriter.class)) : Option.empty();
    if (expectedFailure.startsWith("Pre-commit")) {
      props.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), SparkWriteErrorValidator.class.getName());
    }
    StreamSync sync = new StreamSync(cfg, mock(SparkSession.class), props, context,
        mock(HoodieStorage.class), new Configuration(), client -> true, null,
        errorTable, mock(SourceFormatAdapter.class), Option.empty(), false);
    SparkRDDWriteClient client = mock(SparkRDDWriteClient.class);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tempDir.toString()).build();
    when(client.getConfig()).thenReturn(config);
    when(client.startCommit(anyString(), any())).thenReturn("001");
    JavaRDD<WriteStatus> statuses = mock(JavaRDD.class);
    WriteStatus status = new WriteStatus();
    status.setTotalRecords(1);
    status.setTotalErrorRecords(1);
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p");
    stat.setTotalWriteErrors(1);
    status.setStat(stat);
    when(statuses.getStorageLevel()).thenReturn(StorageLevel.MEMORY_ONLY());
    when(statuses.collect()).thenReturn(Collections.singletonList(status));
    when(client.insert(any(), anyString())).thenReturn(statuses);
    when(context.emptyRDD()).thenReturn(mock(JavaRDD.class));
    HoodieRollbackException rollbackFailure = new HoodieRollbackException("rollback failed");
    if (rollbackFails) {
      when(client.rollback("001")).thenThrow(rollbackFailure);
    }
    setField(sync, "writeClient", client);
    setField(sync, "errorWriteFailureStrategy", HoodieErrorTableConfig.ErrorWriteFailureStrategy.ROLLBACK_COMMIT);
    setField(sync, "commitsTimelineOpt", Option.of(metaClient.getActiveTimeline().getCommitsTimeline()));
    InputBatch batch = new InputBatch(Option.empty(), null, null);
    Method write = StreamSync.class.getDeclaredMethod("writeToSinkAndDoMetaSync", HoodieTableMetaClient.class,
        InputBatch.class, boolean.class, HoodieIngestionMetrics.class, Timer.Context.class);
    write.setAccessible(true);
    InvocationTargetException invocation = assertThrows(InvocationTargetException.class,
        () -> write.invoke(sync, metaClient, batch, false, mock(HoodieIngestionMetrics.class), null));
    Throwable failure = invocation.getCause();
    if (!(failure instanceof HoodieStreamerWriteException)) {
      throw new AssertionError("Expected original streamer failure", failure);
    }
    assertTrue(failure.getMessage().contains(expectedFailure));
    if (expectedFailure.startsWith("Pre-commit")) {
      assertInstanceOf(HoodieValidationException.class, failure.getCause());
    }
    assertArrayEquals(rollbackFails ? new Throwable[] {rollbackFailure} : new Throwable[0], failure.getSuppressed());
  }

  private static void setField(StreamSync sync, String name, Object value) throws Exception {
    Field field = StreamSync.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(sync, value);
  }

  @Test
  void rolledBackReturnsTheOriginalFailureWithTheRollbackFailureSuppressed() {
    HoodieStreamerWriteException failure = new HoodieStreamerWriteException("original");
    HoodieRollbackException rollbackFailure = new HoodieRollbackException("rollback failed");
    assertSame(failure, StreamSync.rolledBack(() -> {
      throw rollbackFailure;
    }, failure));
    assertEquals(1, failure.getSuppressed().length);
    assertSame(rollbackFailure, failure.getSuppressed()[0]);
  }

  @Test
  void rolledBackRunsTheRollbackAndAddsNothingWhenItSucceeds() {
    HoodieStreamerWriteException failure = new HoodieStreamerWriteException("original");
    int[] rollbacks = new int[1];
    assertSame(failure, StreamSync.rolledBack(() -> rollbacks[0]++, failure));
    assertEquals(1, rollbacks[0]);
    assertEquals(0, failure.getSuppressed().length);
  }
}
