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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDeleteHelper {

  private enum CombineTestMode {
    None, GlobalIndex, NoneGlobalIndex;
  }

  private static final String BASE_PATH = "/tmp/";
  private static final boolean WITH_COMBINE = true;
  private static final boolean WITHOUT_COMBINE = false;
  private static final int DELETE_PARALLELISM = 200;

  @Mock
  private HoodieBloomIndex index;
  @Mock
  private HoodieTable<EmptyHoodieRecordPayload, JavaRDD<HoodieRecord>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table;
  @Mock
  private BaseSparkCommitActionExecutor<EmptyHoodieRecordPayload> executor;
  @Mock
  private HoodieWriteMetadata metadata;
  @Mock
  private JavaPairRDD keyPairs;
  @Mock
  private JavaSparkContext jsc;
  @Mock
  private HoodieSparkEngineContext context;

  private JavaRDD<HoodieKey> rddToDelete;
  private HoodieWriteConfig config;

  @BeforeEach
  public void setUp() {
    when(table.getIndex()).thenReturn(index);
    when(context.getJavaSparkContext()).thenReturn(jsc);
  }

  @Test
  public void deleteWithEmptyRDDShouldNotExecute() {
    rddToDelete = mockEmptyHoodieKeyRdd();
    config = newWriteConfig(WITHOUT_COMBINE);

    SparkDeleteHelper.newInstance().execute("test-time", rddToDelete, context, config, table, executor);

    verify(rddToDelete, never()).repartition(DELETE_PARALLELISM);
    verifyNoDeleteExecution();
  }

  @Test
  public void deleteWithoutCombineShouldRepartitionForNonEmptyRdd() {
    rddToDelete = newHoodieKeysRddMock(2, CombineTestMode.None);
    config = newWriteConfig(WITHOUT_COMBINE);

    SparkDeleteHelper.newInstance().execute("test-time", rddToDelete, context, config, table, executor);

    verify(rddToDelete, times(1)).repartition(DELETE_PARALLELISM);
    verifyDeleteExecution();
  }

  @Test
  public void deleteWithCombineShouldRepartitionForNonEmptyRddAndNonGlobalIndex() {
    rddToDelete = newHoodieKeysRddMock(2, CombineTestMode.NoneGlobalIndex);
    config = newWriteConfig(WITH_COMBINE);

    SparkDeleteHelper.newInstance().execute("test-time", rddToDelete, context, config, table, executor);

    verify(rddToDelete, times(1)).distinct(DELETE_PARALLELISM);
    verifyDeleteExecution();
  }

  @Test
  public void deleteWithCombineShouldRepartitionForNonEmptyRddAndGlobalIndex() {
    rddToDelete = newHoodieKeysRddMock(2, CombineTestMode.GlobalIndex);
    config = newWriteConfig(WITH_COMBINE);
    when(index.isGlobal()).thenReturn(true);

    SparkDeleteHelper.newInstance().execute("test-time", rddToDelete, context, config, table, executor);

    verify(keyPairs, times(1)).reduceByKey(any(), eq(DELETE_PARALLELISM));
    verifyDeleteExecution();
  }

  private void verifyDeleteExecution() {
    verify(executor, times(1)).execute(any());
    verify(metadata, times(1)).setIndexLookupDuration(any());
  }

  private void verifyNoDeleteExecution() {
    verify(executor, never()).execute(any());
  }

  private HoodieWriteConfig newWriteConfig(boolean combine) {
    return HoodieWriteConfig.newBuilder()
            .combineDeleteInput(combine)
            .withPath(BASE_PATH)
            .withDeleteParallelism(DELETE_PARALLELISM)
            .build();
  }

  private JavaRDD<HoodieKey> newHoodieKeysRddMock(int howMany, CombineTestMode combineMode) {
    JavaRDD<HoodieKey> keysToDelete = mock(JavaRDD.class);

    JavaRDD recordsRdd = mock(JavaRDD.class);
    when(recordsRdd.filter(any())).thenReturn(recordsRdd);
    when(recordsRdd.isEmpty()).thenReturn(howMany <= 0);
    when(index.tagLocation(any(), any(), any())).thenReturn(HoodieJavaRDD.of(recordsRdd));

    if (combineMode == CombineTestMode.GlobalIndex) {
      when(keyPairs.reduceByKey(any(), anyInt())).thenReturn(keyPairs);
      when(keyPairs.values()).thenReturn(keysToDelete);
      when(keysToDelete.keyBy(any())).thenReturn(keyPairs);
    } else if (combineMode == CombineTestMode.NoneGlobalIndex) {
      when(keysToDelete.distinct(anyInt())).thenReturn(keysToDelete);
    } else if (combineMode == CombineTestMode.None) {
      List<Partition> parts = mock(List.class);
      when(parts.isEmpty()).thenReturn(howMany <= 0);
      when(keysToDelete.repartition(anyInt())).thenReturn(keysToDelete);
      when(keysToDelete.partitions()).thenReturn(parts);
    }

    when(keysToDelete.map(any())).thenReturn(recordsRdd);
    when(executor.execute(any())).thenReturn(metadata);
    return keysToDelete;
  }

  private JavaRDD<HoodieKey> mockEmptyHoodieKeyRdd() {
    JavaRDD<HoodieKey> emptyRdd = mock(JavaRDD.class);
    doReturn(true).when(emptyRdd).isEmpty();
    doReturn(Collections.emptyList()).when(emptyRdd).partitions();
    doReturn(emptyRdd).when(emptyRdd).map(any());

    doReturn(HoodieJavaRDD.of(emptyRdd)).when(index).tagLocation(any(), any(), any());
    doReturn(emptyRdd).when(emptyRdd).filter(any());

    doNothing().when(executor).saveWorkloadProfileMetadataToInflight(any(), anyString());
    doReturn(emptyRdd).when(jsc).emptyRDD();
    return emptyRdd;
  }

}
