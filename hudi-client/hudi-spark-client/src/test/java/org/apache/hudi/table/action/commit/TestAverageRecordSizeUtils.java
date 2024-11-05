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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test average record size estimation.
 */
public class TestAverageRecordSizeUtils {

  private final HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
  private static final String PARTITION1 = "partition1";
  private static final String TEST_WRITE_TOKEN = "1-0-1";

  @ParameterizedTest
  @MethodSource("testCases")
  public void testAverageRecordSize(List<Pair<HoodieInstant, List<HWriteStat>>> instantSizePairs, long expectedSize) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .build();
    HoodieDefaultTimeline commitsTimeline = new HoodieDefaultTimeline();
    List<HoodieInstant> instants = new ArrayList<>();
    instantSizePairs.forEach(entry -> {
      HoodieInstant hoodieInstant = entry.getKey();
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      entry.getValue().forEach(hWriteStat -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setNumWrites(hWriteStat.getTotalRecordsWritten());
        writeStat.setTotalWriteBytes(hWriteStat.getPerRecordSize() * hWriteStat.getTotalRecordsWritten());
        writeStat.setPath(hWriteStat.getPath());
        commitMetadata.addWriteStat(PARTITION1, writeStat);
      });
      instants.add(hoodieInstant);
      try {
        when(mockTimeline.getInstantDetails(hoodieInstant)).thenReturn(Option.of(getUTF8Bytes(commitMetadata.toJsonString())));
      } catch (IOException e) {
        throw new RuntimeException("Should not have failed", e);
      }
    });

    List<HoodieInstant> reverseOrderInstants = new ArrayList<>(instants);
    Collections.reverse(reverseOrderInstants);
    when(mockTimeline.getInstants()).thenReturn(instants);
    when(mockTimeline.getReverseOrderedInstants()).then(i -> reverseOrderInstants.stream());
    commitsTimeline.setInstants(instants);

    assertEquals(expectedSize, AverageRecordSizeUtils.averageBytesPerRecord(mockTimeline, writeConfig));
  }

  @Test
  public void testErrorHandling() {
    int recordSize = 10000;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(Collections.singletonMap(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(recordSize)))
        .build(false);
    HoodieDefaultTimeline commitsTimeline = new HoodieDefaultTimeline();
    List<HoodieInstant> instants = Collections.singletonList(
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1"));

    when(mockTimeline.getInstants()).thenReturn(instants);
    when(mockTimeline.getReverseOrderedInstants()).then(i -> instants.stream());
    // Simulate a case where the instant details are absent
    commitsTimeline.setInstants(new ArrayList<>());

    assertEquals(recordSize, AverageRecordSizeUtils.averageBytesPerRecord(mockTimeline, writeConfig));
  }

  private static String getBaseFileName(String instantTime) {
    String fileName = UUID.randomUUID().toString();
    return FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, PARQUET.getFileExtension());
  }

  private static String getLogFileName(String instantTime) {
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, PARQUET.getFileExtension());
    assertEquals(instantTime, FSUtils.getCommitTime(fullFileName));
    return FSUtils.makeLogFileName(fileName, HOODIE_LOG.getFileExtension(), instantTime, 1, TEST_WRITE_TOKEN);
  }

  static Stream<Arguments> testCases() {
    Long baseInstant = 20231204194919610L;
    List<Arguments> arguments = new ArrayList<>();
    // COW
    // straight forward. just 1 instant.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L)))), 100L));

    // two instants. latest instant should be honored
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 100)), 10000000L, 200L)))), 200L));

    // two instants, while 2nd one is smaller in size so as to not meet the threshold. So, 1st one should be honored
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 100)), 10000L, 200L)))), 100L));

    // 2nd instance is replace commit and should be honored.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 100)), 10000000L, 200L)))), 200L));

    // MOR
    // for delta commits, only parquet files should be accounted for.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 100)), 10000000L, 200L)))), 200L));

    // delta commit has a mix of parquet and log files. only parquet files should be accounted for.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 1000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 100)), 10000000L, 200L),
                    new HWriteStat(getLogFileName(String.valueOf(baseInstant + 100)), 10000000L, 300L)))), 200L));

    // 2nd delta commit only has log files. and so we honor 1st delta commit size.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 10000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(new HWriteStat(getLogFileName(String.valueOf(baseInstant + 100)), 1000000L, 200L),
                    new HWriteStat(getLogFileName(String.valueOf(baseInstant + 100)), 10000000L, 300L)))), 100L));

    // replace commit should be honored.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant)), 1000000L, 100L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(new HWriteStat(getLogFileName(String.valueOf(baseInstant + 100)), 1000000L, 200L),
                    new HWriteStat(getLogFileName(String.valueOf(baseInstant + 100)), 1000000L, 300L))),
            Pair.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(new HWriteStat(getBaseFileName(String.valueOf(baseInstant + 200)), 1000000L, 400L)))), 400L));
    return arguments.stream();
  }

  static class HWriteStat {
    private final String path;
    private final Long totalRecordsWritten;
    private final Long perRecordSize;

    public HWriteStat(String path, Long totalRecordsWritten, Long perRecordSize) {
      this.path = path;
      this.totalRecordsWritten = totalRecordsWritten;
      this.perRecordSize = perRecordSize;
    }

    public String getPath() {
      return path;
    }

    public Long getTotalRecordsWritten() {
      return totalRecordsWritten;
    }

    public Long getPerRecordSize() {
      return perRecordSize;
    }
  }
}
