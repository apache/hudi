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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.InputBatch;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The real streamer must consume error statuses before a commit releases their write cache. */
@Tag("functional")
class TestStreamSyncErrorStatusOrder extends SparkClientFunctionalTestHarness {
  @TempDir
  Path tablePath;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void errorWriteRunsOnceWhetherCommitKeepsOrReleasesItsStatuses(boolean releasesCache) throws Exception {
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath.toString());
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tablePath.toString();
    cfg.tableType = "COPY_ON_WRITE";
    cfg.operation = WriteOperationType.INSERT;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.checkpoint.force.skip", "true");
    props.setProperty(HoodieErrorTableConfig.ENABLE_ERROR_TABLE_WRITE_UNIFICATION.key(), "true");
    LongAccumulator writes = spark().sparkContext().longAccumulator("error-write-evaluations");
    JavaRDD<WriteStatus> errorStatuses = jsc().parallelize(Arrays.asList(0, 1), 2).map(partition -> {
      writes.add(10);
      WriteStatus status = new WriteStatus();
      status.setTotalRecords(10);
      return status;
    }).persist(StorageLevel.MEMORY_ONLY());
    BaseErrorTableWriter writer = mock(BaseErrorTableWriter.class);
    when(writer.upsert(anyString(), any())).thenReturn(errorStatuses);
    when(writer.commit(any())).thenAnswer(invocation -> {
      JavaRDD<WriteStatus> statuses = invocation.getArgument(0);
      statuses.count();
      if (releasesCache) {
        statuses.unpersist(true);
      }
      return true;
    });
    StreamSync sync = new StreamSync(cfg, spark(), props, context(), meta.getStorage(), new Configuration(), client -> true, null,
        Option.of(writer), mock(SourceFormatAdapter.class), Option.empty(), false);
    SparkRDDWriteClient client = mock(SparkRDDWriteClient.class);
    when(client.getConfig()).thenReturn(HoodieWriteConfig.newBuilder().withPath(tablePath.toString()).build());
    when(client.startCommit(anyString(), any())).thenReturn("001");
    when(client.rollback("001")).thenReturn(true);
    WriteStatus data = new WriteStatus();
    data.setTotalRecords(1);
    data.setTotalErrorRecords(1);
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p");
    stat.setTotalWriteErrors(1);
    data.setStat(stat);
    JavaRDD<WriteStatus> dataStatuses = jsc().parallelize(Collections.singletonList(data), 1);
    when(client.insert(any(), anyString())).thenReturn(dataStatuses);
    setField(sync, "writeClient", client);
    setField(sync, "commitsTimelineOpt", Option.of(meta.getActiveTimeline().getCommitsTimeline()));
    setField(sync, "errorWriteFailureStrategy", HoodieErrorTableConfig.ErrorWriteFailureStrategy.ROLLBACK_COMMIT);
    Method write = StreamSync.class.getDeclaredMethod("writeToSinkAndDoMetaSync", HoodieTableMetaClient.class,
        InputBatch.class, boolean.class, HoodieIngestionMetrics.class, Timer.Context.class);
    write.setAccessible(true);
    try {
      InvocationTargetException failure = assertThrows(InvocationTargetException.class,
          () -> write.invoke(sync, meta, new InputBatch(Option.empty(), null, null), false, mock(HoodieIngestionMetrics.class), null));
      assertTrue(failure.getCause().getMessage().contains("write errors"), failure.getCause().toString());
      assertEquals(20L, (long) writes.value(), "counting after cache release must not execute the error write again");
    } finally {
      dataStatuses.unpersist(true);
      errorStatuses.unpersist(true);
      sync.close();
    }
  }

  private static void setField(StreamSync sync, String name, Object value) throws Exception {
    Field field = StreamSync.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(sync, value);
  }
}
