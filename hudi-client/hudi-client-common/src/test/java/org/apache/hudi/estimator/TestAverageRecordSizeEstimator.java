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

package org.apache.hudi.estimator;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.BaseTimelineV2;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
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
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_COMPARATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test average record size estimation.
 */
public class TestAverageRecordSizeEstimator {
  private static final String BASE_FILE_EXTENSION =
      HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
  private final HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
  private final CommitMetadataSerDe mockCommitMetadataSerDe = mock(CommitMetadataSerDe.class);
  private static final String PARTITION1 = "partition1";
  private static final String TEST_WRITE_TOKEN = "1-0-1";
  private static final Integer DEFAULT_MAX_COMMITS = 2;
  // needs to be big enough to skew the estimate
  private static final Integer DEFAULT_AVERAGE_PARQUET_METADATA_SIZE = 10000000;
  private static final Double DEFAULT_RECORD_SIZE_ESTIMATE_THRESHOLD = 0.1;

  @Test
  public void testAverageBytesPerRecordForEmptyCommitTimeLine() throws Exception {
    HoodieTimeline commitTimeLine = mock(HoodieTimeline.class);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp").build();
    when(commitTimeLine.empty()).thenReturn(true);
    long expectAvgSize = config.getCopyOnWriteRecordSizeEstimate();
    AverageRecordSizeEstimator averageRecordSizeEstimator = new AverageRecordSizeEstimator(config);
    long actualAvgSize = averageRecordSizeEstimator.averageBytesPerRecord(commitTimeLine, COMMIT_METADATA_SER_DE);
    assertEquals(expectAvgSize, actualAvgSize);
  }

  @Test
  public void testErrorHandling() {
    int recordSize = 10000;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(Collections.singletonMap(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(recordSize)))
        .build(false);
    BaseTimelineV2 commitsTimeline = new BaseTimelineV2();
    List<HoodieInstant> instants = Collections.singletonList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1"));

    when(mockTimeline.getInstants()).thenReturn(instants);
    when(mockTimeline.getReverseOrderedInstants()).then(i -> instants.stream());
    // Simulate a case where the instant details are absent
    commitsTimeline.setInstants(new ArrayList<>());
    AverageRecordSizeEstimator averageRecordSizeEstimator = new AverageRecordSizeEstimator(writeConfig);
    long actualAvgSize = averageRecordSizeEstimator.averageBytesPerRecord(mockTimeline, COMMIT_METADATA_SER_DE);
    assertEquals(recordSize, actualAvgSize);
  }

  @ParameterizedTest
  @MethodSource("testCases")
  public void testAverageRecordSizeWithNonEmptyCommitTimeline(List<Pair<HoodieInstant, List<HWriteStat>>> instantSizePairs, long expectedSize) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withRecordSizeEstimator(AverageRecordSizeEstimator.class.getName())
        .withRecordSizeEstimatorMaxCommits(DEFAULT_MAX_COMMITS)
        .withRecordSizeEstimatorAverageMetadataSize(DEFAULT_AVERAGE_PARQUET_METADATA_SIZE)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionRecordSizeEstimateThreshold(DEFAULT_RECORD_SIZE_ESTIMATE_THRESHOLD)
            .build())
        .build();

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
        when(mockTimeline.readCommitMetadata(hoodieInstant)).thenReturn(commitMetadata);
      } catch (IOException e) {
        throw new RuntimeException("Should not have failed", e);
      }
    });

    List<HoodieInstant> reverseOrderInstants = new ArrayList<>(instants);
    Collections.reverse(reverseOrderInstants);

    when(mockTimeline.filterCompletedInstants()).thenReturn(mockTimeline);
    when(mockTimeline.getReverseOrderedInstants()).then(i -> reverseOrderInstants.stream());

    AverageRecordSizeEstimator averageRecordSizeEstimator = new AverageRecordSizeEstimator(writeConfig);
    long actualSize = averageRecordSizeEstimator.averageBytesPerRecord(mockTimeline, mockCommitMetadataSerDe);
    assertEquals(expectedSize, actualSize);
  }

  private static String getBaseFileName(String instantTime) {
    String fileName = UUID.randomUUID().toString();
    return FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, BASE_FILE_EXTENSION);
  }

  private static String getLogFileName(String instantTime) {
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName, BASE_FILE_EXTENSION);
    assertEquals(instantTime, FSUtils.getCommitTime(fullFileName));
    return FSUtils.makeLogFileName(fileName, HOODIE_LOG.getFileExtension(), instantTime, 1, TEST_WRITE_TOKEN);
  }

  private static Stream<Arguments> testCases() {
    Long baseInstant = 20231204194919610L;
    Long standardCount = 10000000L;
    List<Arguments> arguments = new ArrayList<>();
    // Note the avg record estimate is based on a parquet metadata size of 500Bytes per file.
    // 1. straight forward. just 1 instant.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
            Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L)))), 99L));

    // 2. two instants. latest instant should be honored
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, standardCount, 200L)))), 199L));

    // 3. two instants, while 2nd one is smaller in size so as to not meet the threshold. So, 1st one should be honored
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, 1000L, 200L)))), 99L));

    // 4. 2nd instance is replace commit, it should be excluded
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 200L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.REPLACE_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, standardCount, 100L)))), 199L));

    // 5. for delta commits, only parquet files should be accounted for.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, standardCount, 200L)))), 199L));

    // 6. delta commit has a mix of parquet and log files. only parquet files should be accounted for.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(generateBaseWriteStat(baseInstant + 100, standardCount, 200L),
                    generateLogWriteStat(baseInstant + 100, standardCount, 300L)))), 199L));

    // 7. 2nd delta commit only has log files. and so we honor 1st delta commit size.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(generateLogWriteStat(baseInstant + 100, standardCount, 200L),
                    generateLogWriteStat(baseInstant + 100, standardCount, 300L)))), 99L));

    // 8. since default max commits is overriden to 2 commits, ignore the earliest commit here since there are total 3 commits
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 200L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, 1L, 50L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 200)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 200, 1L, 100L)))), Long.valueOf(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.defaultValue())));

    // 9. replace commits should be ignored despite being the latest commits.
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 100L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Arrays.asList(generateLogWriteStat(baseInstant + 100, standardCount, 200L),
                    generateLogWriteStat(baseInstant + 100, 1000000L, 300L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.REPLACE_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 200, standardCount, 2000L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.REPLACE_COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 300, standardCount, 3000L)))), 99L));

    // 10. Ignore commit stat with 0 records
    arguments.add(Arguments.of(
        Arrays.asList(Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant)),
                Collections.singletonList(generateBaseWriteStat(baseInstant, standardCount, 1000L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.COMMIT_ACTION, Long.toString(baseInstant + 100)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 100, standardCount, 50L))),
            Pair.of(generateCompletedInstant(HoodieTimeline.DELTA_COMMIT_ACTION, Long.toString(baseInstant + 200)),
                Collections.singletonList(generateBaseWriteStat(baseInstant + 200, 0L, 1000L)))), 49L));

    return arguments.stream();
  }

  private static HoodieInstant generateCompletedInstant(String action, String instant) {
    return new HoodieInstant(HoodieInstant.State.COMPLETED, action, instant, INSTANT_COMPARATOR.requestedTimeOrderedComparator());
  }

  private static HWriteStat generateBaseWriteStat(long instant, long totalRecordsWritten, long perRecordSize) {
    return new HWriteStat(getBaseFileName(String.valueOf(instant)), totalRecordsWritten, perRecordSize);
  }

  private static HWriteStat generateLogWriteStat(long instant, long totalRecordsWritten, long perRecordSize) {
    return new HWriteStat(getLogFileName(String.valueOf(instant)), totalRecordsWritten, perRecordSize);
  }

  @AllArgsConstructor
  @Getter
  static class HWriteStat {

    private final String path;
    private final Long totalRecordsWritten;
    private final Long perRecordSize;
  }
}
