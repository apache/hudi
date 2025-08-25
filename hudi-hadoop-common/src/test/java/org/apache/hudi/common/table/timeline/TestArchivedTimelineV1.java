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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.v1.ArchivedTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantGeneratorV1;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ArchivedTimelineV1}.
 */
public class TestArchivedTimelineV1 extends HoodieCommonTestHarness {

  private static final InstantGeneratorV1 INSTANT_GENERATOR_V1 = new InstantGeneratorV1();
  private HoodieArchivedTimeline timeline;

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void clean() {
    cleanMetaClient();
  }

  @Test
  public void testArchivedInstantsNotLoadedToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();
    // Creating a timeline without time range filter does not load instants to memory
    timeline = new ArchivedTimelineV1(metaClient);

    // Test that hoodie archived timeline does not load instants to memory by default
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "08", "09", "11"), false);
    assertEquals(instants, timeline.getInstants());
  }

  @Test
  public void testLoadArchivedInstantsInStartTsRangeToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new ArchivedTimelineV1(metaClient, "08");

    // Note that instant 11 should not be loaded as it is not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("08", "09"), true);

    // Timeline should only keep completed instants
    List<HoodieInstant> completedInstants = getCompletedInstantForTs(instants, Arrays.asList("08", "09"));
    assertEquals(completedInstants, timeline.getInstants());
  }

  @Test
  public void testLoadArchivedInstantsInInclusiveTsRangeToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new ArchivedTimelineV1(metaClient, "05", "09");

    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("05", "08", "09"), true);

    List<HoodieInstant> completedInstants = getCompletedInstantForTs(instants, Arrays.asList("05", "08", "09"));
    assertEquals(completedInstants, timeline.getInstants());
  }

  @Test
  public void testLoadArchivedCompletedInstantsToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new ArchivedTimelineV1(metaClient, "01", "11");

    // Instants 01 and 11 should not be loaded to memory since they are not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true);

    // All the completed instants should be returned
    assertEquals(instants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList()), timeline.getInstants());
  }

  @Test
  public void testLoadArchivedCompactionInstantsToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new ArchivedTimelineV1(metaClient);
    timeline.loadCompactionDetailsInMemory("08");

    // Only compaction commit (timestamp 08) should be loaded to memory
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "09", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("08"), true, Arrays.asList(INFLIGHT));

    // All the instants should be returned although only compaction instants should be loaded to memory
    assertEquals(instants, timeline.getInstants());
  }

  @Test
  public void testLoadArchivedInstantsToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new ArchivedTimelineV1(metaClient);
    timeline.loadInstantDetailsInMemory("02", "11");

    // Instants 03, 05, 08, 09 and completed states and 11 has requested and inflight states
    // loadInstantDetailsInMemory load instants of all states
    // 03 does not load inflight because it is replace commit
    validateInstantsLoaded(timeline, Arrays.asList("03"), true, Arrays.asList(REQUESTED, COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("03"), false, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(
        timeline, Arrays.asList("05", "08", "09"), true, Arrays.asList(REQUESTED, INFLIGHT, COMPLETED));
    validateInstantsLoaded(timeline, Collections.singletonList("11"), true, Arrays.asList(REQUESTED, INFLIGHT));

    // All the instants should be returned although only compaction instants should be loaded to memory
    assertEquals(instants, timeline.getInstants());
  }

  @Test
  public void testLoadAllArchivedCompletedInstantsByLogFilePaths() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(metaClient, new HashSet<>(archivedLogFilePaths));

    // Instants 01 and 11 should not be loaded to memory since they are not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true);

    // All the completed instants should be returned
    assertEquals(instants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList()), timeline.getInstants());
  }

  @Test
  public void testLoadFilteredArchivedCompletedInstantsBySingleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(metaClient, Collections.singleton(archivedLogFilePaths.get(0)));

    // Only Instant 03 of completed state should be loaded to memory (since they are in log file 0)
    validateInstantsLoaded(timeline, Collections.singletonList("03"), true, Collections.singletonList(COMPLETED));
    validateInstantsLoaded(timeline, Collections.singletonList("03"), false, Arrays.asList(REQUESTED, INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isCompleted)
        .filter(instant -> Collections.singletonList("03").contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    // to make sure only completed instants are returned
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isInflight).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isRequested).count());
  }

  @Test
  public void testLoadFilteredArchivedRequestedInstantsBySingleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();
    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(
        metaClient, Collections.singleton(archivedLogFilePaths.get(0)), Option.of(REQUESTED));

    // Instants 01, 03, 05 of requested states should be loaded to memory (since they are in log file 0)
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05"), true, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05"), false, Arrays.asList(INFLIGHT, COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("08", "09", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isRequested)
            .filter(instant -> new HashSet<>(Arrays.asList("01", "03", "05")).contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    // to make sure only requested instants are returned
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isCompleted).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isInflight).count());
  }

  @Test
  public void testLoadFilteredArchivedInflightInstantsBySingleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();
    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(
        metaClient, Collections.singleton(archivedLogFilePaths.get(0)), Option.of(INFLIGHT));

    // Only inflight instant of 01 should be loaded to memory
    // Since 03 is replacecommit, 03 inflight will not be loaded (refer to MetadataConversionUtils.java)
    validateInstantsLoaded(timeline, Arrays.asList("01"), true, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("01"), false, Arrays.asList(REQUESTED, COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("03"), false);
    validateInstantsLoaded(timeline, Arrays.asList("05", "08", "09", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isInflight)
            .filter(instant -> new HashSet<>(Arrays.asList("01", "03")).contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    // to make sure only inflight instants are loaded
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isCompleted).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isRequested).count());
  }

  @Test
  public void testLoadFilteredArchivedAllInstantsBySingleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();
    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(metaClient, Collections.singleton(archivedLogFilePaths.get(0)), Option.empty());

    // Instant 03 requested and completed should be loaded (since 03 is replace commit)
    validateInstantsLoaded(timeline, Collections.singletonList("03"), true, Arrays.asList(REQUESTED, COMPLETED));
    validateInstantsLoaded(timeline, Collections.singletonList("03"), false, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(timeline, Collections.singletonList("01"), true, Arrays.asList(REQUESTED, INFLIGHT));
    validateInstantsLoaded(timeline, Collections.singletonList("01"), false, Collections.singletonList(COMPLETED));
    validateInstantsLoaded(timeline, Collections.singletonList("05"), true, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, Collections.singletonList("05"), false, Arrays.asList(INFLIGHT, COMPLETED));

    // Instants 01, 05 should be loaded to memory (since they are in log file 0)
    validateInstantsLoaded(timeline, Arrays.asList("01", "05"), true);
    validateInstantsLoaded(timeline, Arrays.asList("08", "09", "11"), false);

    assertEquals(instants.subList(0, 6), timeline.getInstants());
  }

  @Test
  public void testLoadFilteredArchivedCompletedInstantsByMultipleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(metaClient, new HashSet<>(archivedLogFilePaths.subList(0, 2)));

    // Instants 03, 05, 08, 09 of completed state should be loaded to memory (since they are in log file 0 and 1)
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true, Collections.singletonList(COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), false, Arrays.asList(REQUESTED, INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isCompleted)
            .filter(instant -> Arrays.asList("03", "05", "08", "09").contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isRequested).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isInflight).count());
  }

  @Test
  public void testLoadFilteredArchivedRequestedInstantsByMultipleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(
        metaClient, new HashSet<>(archivedLogFilePaths.subList(0, 2)), Option.of(REQUESTED));

    // Instant 03 requested should be loaded (since 03 is replace commit)
    validateInstantsLoaded(timeline, Collections.singletonList("03"), true, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, Collections.singletonList("03"), false, Arrays.asList(INFLIGHT, COMPLETED));
    // Instants 01, 05, 08, 09 should be loaded to memory (since they are in log file 0 and 1)
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09"), true, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09"), false, Arrays.asList(INFLIGHT, COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isRequested)
            .filter(instant -> Arrays.asList("01", "03", "05", "08", "09").contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isCompleted).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isInflight).count());
  }

  @Test
  public void testLoadFilteredArchivedInflightInstantsByMultipleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(
        metaClient, new HashSet<>(archivedLogFilePaths.subList(0, 2)), Option.of(INFLIGHT));

    // Instant 03 should not be loaded since it is a replace commit
    validateInstantsLoaded(timeline, Collections.singletonList("03"), false);
    // Instants 01, 05, 08, 09 should be loaded to memory (since they are in log file 0 and 1)
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09"), true, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isInflight)
            .filter(instant -> Arrays.asList("01", "03", "05", "08", "09").contains(instant.requestedTime())).collect(Collectors.toList()),
        timeline.getInstants());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isCompleted).count());
    assertEquals(0, timeline.getInstants().stream().filter(HoodieInstant::isRequested).count());
  }

  @Test
  public void testLoadFilteredArchivedAllInstantsByMultipleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new ArchivedTimelineV1(metaClient, new HashSet<>(archivedLogFilePaths.subList(0, 2)), Option.empty());

    // Instant 03 requested and completed should be loaded (since 03 is replace commit)
    validateInstantsLoaded(timeline, Collections.singletonList("03"), true, Arrays.asList(REQUESTED, COMPLETED));
    validateInstantsLoaded(timeline, Collections.singletonList("03"), false, Collections.singletonList(INFLIGHT));
    // Instants 01, 05, 08, 09 should be loaded to memory (since they are in log file 0 and 1)
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09"), true);
    validateInstantsLoaded(timeline, Arrays.asList("11"), false);

    assertEquals(instants.subList(0, 14), timeline.getInstants());
  }

  @Test
  public void testLoadArchivedCompletedInstantsForAdditionalActions() throws Exception {
    List<HoodieInstant> instants = createAdditionalInstants();
    timeline = new ArchivedTimelineV1(metaClient, "15", "21");

    List<String> timestamps = Arrays.asList("15", "17", "19", "21");
    validateInstantsLoaded(timeline, timestamps, true, Collections.singletonList(COMPLETED));
    validateInstantsLoaded(timeline, timestamps, false, Arrays.asList(REQUESTED, INFLIGHT));

    assertEquals(getCompletedInstantForTs(instants, timestamps), timeline.getInstants());
  }

  @Test
  public void testLoadArchivedRequestedInstantsForAdditionalActions() throws Exception {
    List<HoodieInstant> instants = createAdditionalInstants();
    timeline = new ArchivedTimelineV1(metaClient, "15", "21", Option.of(REQUESTED));

    /**
     * According to {@link org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.getMetadataKey},
     * requested instants are not loaded for CLEAN and ROLLBACK actions
     */
    List<String> timestamps = Arrays.asList("15", "17", "19", "21");
    validateInstantsLoaded(timeline, Arrays.asList("17", "21"), true, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, Arrays.asList("15", "19"), false, Collections.singletonList(REQUESTED));
    validateInstantsLoaded(timeline, timestamps, false, Arrays.asList(INFLIGHT, COMPLETED));

    assertEquals(instants.stream().filter(HoodieInstant::isRequested).collect(Collectors.toList()), timeline.getInstants());
  }

  @Test
  public void testLoadArchivedInflightInstantsForAdditionalActions() throws Exception {
    List<HoodieInstant> instants = createAdditionalInstants();
    timeline = new ArchivedTimelineV1(metaClient, "15", "21", Option.of(INFLIGHT));

    /**
     * According to {@link org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.getMetadataKey},
     * inflight instants are not loaded for CLEAN and ROLLBACK actions
     */
    List<String> timestamps = Arrays.asList("15", "17", "19", "21");
    validateInstantsLoaded(timeline, Arrays.asList("17", "21"), true, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("15", "19"), false, Collections.singletonList(INFLIGHT));
    validateInstantsLoaded(timeline, timestamps, false, Arrays.asList(REQUESTED, COMPLETED));

    assertEquals(instants.stream().filter(HoodieInstant::isInflight).collect(Collectors.toList()), timeline.getInstants());
  }

  @Test
  public void testLoadArchivedInstantsForAdditionalActions() throws Exception {
    List<HoodieInstant> instants = createAdditionalInstants();
    timeline = new ArchivedTimelineV1(metaClient, "15", "21", Option.empty());

    List<String> timestamps = Arrays.asList("15", "17", "19", "21");
    // For CLEAN (ts 15) and ROLLBACK (ts 19) actions, only completed instants are loaded
    validateInstantsLoaded(timeline, Arrays.asList("15", "19"), true, Collections.singletonList(COMPLETED));
    validateInstantsLoaded(timeline, Arrays.asList("15", "19"), false, Arrays.asList(REQUESTED, INFLIGHT));
    validateInstantsLoaded(timeline, Arrays.asList("17", "21"), true);

    assertEquals(instants, timeline.getInstants());
  }

  @Test
  void readLegacyArchivedTimelineWithNullFields() {
    String samplePath = this.getClass().getClassLoader().getResource("archived_timeline").getPath();
    metaClient = HoodieTableMetaClient.builder()
        .setBasePath(samplePath)
        .setConf(getDefaultStorageConf())
        .setLoadActiveTimelineOnLoad(false)
        .build();
    HoodieArchivedTimeline timeline = new ArchivedTimelineV1(metaClient);
    timeline.loadCompletedInstantDetailsInMemory();
    assertEquals(3, timeline.getInstants().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"hudi_0_13_0_archive", "hudi_0_8_0_archive"})
  void shouldReadArchivedFileAndValidateContent(String archivePath) {
    String path = this.getClass().getClassLoader().getResource(archivePath).getPath() + "/.commits_.archive.1_1-0-1";
    assertDoesNotThrow(() -> readAndValidateArchivedFile(path, metaClient.getStorage()));
  }

  private void readAndValidateArchivedFile(String path, HoodieStorage storage) throws IOException {
    try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(
        storage, new HoodieLogFile(path), HoodieArchivedMetaEntry.getClassSchema())) {

      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (block instanceof HoodieAvroDataBlock) {
          HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
          try (ClosableIterator<HoodieRecord<IndexedRecord>> itr =
                   avroBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
            if (itr.hasNext()) {
              itr.next();
            }
          }
        }
      }
    }
  }

  /**
   * Validate whether the instants of given timestamps of the hudi archived timeline are loaded to memory or not.
   * @param hoodieArchivedTimeline archived timeline to test against
   * @param instantTsList list of instant timestamps to validate
   * @param isInstantLoaded flag to check whether the instants are loaded to memory or not
   */
  private void validateInstantsLoaded(HoodieArchivedTimeline hoodieArchivedTimeline, List<String> instantTsList, boolean isInstantLoaded) {
    Set<String> instantTsSet = new HashSet<>(instantTsList);
    timeline.getInstants().stream().filter(instant -> instantTsSet.contains(instant.requestedTime())).forEach(instant -> {
      if (isInstantLoaded) {
        assertTrue(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      } else {
        assertFalse(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      }
    });
  }

  /**
   * Validate whether the instants of given timestamps of the hudi archived timeline are loaded to memory or not.
   * @param hoodieArchivedTimeline archived timeline to test against
   * @param instantTsList list of instant timestamps to validate
   * @param isInstantLoaded flag to check whether the instants are loaded to memory or not
   * @param acceptableStates list of states to filter the instants to validate
   */
  private void validateInstantsLoaded(HoodieArchivedTimeline hoodieArchivedTimeline, List<String> instantTsList, boolean isInstantLoaded, List<HoodieInstant.State> acceptableStates) {
    if (acceptableStates.isEmpty()) {
      validateInstantsLoaded(hoodieArchivedTimeline, instantTsList, isInstantLoaded);
      return;
    }

    // Get instant from timeline corresponding to instantTs and instantState
    Map<String, String> instantTsToStateMap = getInstantTsToActionMap();
    List<HoodieInstant> instantsToTest = new ArrayList<>();
    for (String ts : instantTsList) {
      for (HoodieInstant.State state : acceptableStates) {
        instantsToTest.add(new HoodieInstant(state, instantTsToStateMap.get(ts), ts, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR));
      }
    }

    for (HoodieInstant instant : instantsToTest) {
      if (isInstantLoaded) {
        assertTrue(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      } else {
        assertFalse(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      }
    }
  }

  /**
   * Get list of completed hoodie instants for given timestamps.
   */
  private List<HoodieInstant> getCompletedInstantForTs(List<HoodieInstant> instants, List<String> instantTsList) {
    return instants.stream().filter(HoodieInstant::isCompleted)
        .filter(instant -> (new HashSet<>(instantTsList).contains(instant.requestedTime()))).collect(Collectors.toList());
  }

  /**
   * Creates a map of instant timestamp to action use for testing.
   */
  private Map<String, String> getInstantTsToActionMap() {
    return new HashMap<String, String>() {{
        put("01", HoodieTimeline.COMMIT_ACTION);
        put("03", HoodieTimeline.REPLACE_COMMIT_ACTION);
        put("05", HoodieTimeline.COMMIT_ACTION);
        put("08", HoodieTimeline.COMPACTION_ACTION);
        put("09", HoodieTimeline.COMMIT_ACTION);
        put("11", HoodieTimeline.COMMIT_ACTION);
        put("15", HoodieTimeline.CLEAN_ACTION);
        put("17", HoodieTimeline.DELTA_COMMIT_ACTION);
        put("19", HoodieTimeline.ROLLBACK_ACTION);
        put("21", HoodieTimeline.COMPACTION_ACTION);
      }};
  }

  /**
   * Create instants for testing. If archiveInstants is true, create archived commits.
   * archived commit 1 - instant1, instant2
   * archived commit 2 - instant3, instant4, instant5
   * archived commit 3 - instant6
   *
   * log file 1 - 01 (inflight), 03 (complete), and 05 (inflight)
   * log file 2 - instant 05 (complete), 08 (complete), and 09 (complete)
   * log file 3 - instant 11 (inflight)
   */
  private List<HoodieInstant> createInstants() throws Exception {
    HoodieInstant instant1Requested = createInstantV1(REQUESTED, HoodieTimeline.COMMIT_ACTION, "01");
    HoodieInstant instant1Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMMIT_ACTION, "01");

    HoodieInstant instant2Requested = createInstantV1(REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "03");
    HoodieInstant instant2Inflight = createInstantV1(INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, "03");
    HoodieInstant instant2Complete = createInstantV1(COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "03");

    HoodieInstant instant3Requested = createInstantV1(REQUESTED, HoodieTimeline.COMMIT_ACTION, "05");
    HoodieInstant instant3Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMMIT_ACTION, "05");
    HoodieInstant instant3Complete = createInstantV1(COMPLETED, HoodieTimeline.COMMIT_ACTION, "05");

    HoodieInstant instant4Requested = createInstantV1(REQUESTED, HoodieTimeline.COMPACTION_ACTION, "08");
    HoodieInstant instant4Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "08");
    HoodieInstant instant4Complete = createInstantV1(COMPLETED, HoodieTimeline.COMPACTION_ACTION, "08");

    HoodieInstant instant5Requested = createInstantV1(REQUESTED, HoodieTimeline.COMMIT_ACTION, "09");
    HoodieInstant instant5Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMMIT_ACTION, "09");
    HoodieInstant instant5Complete = createInstantV1(COMPLETED, HoodieTimeline.COMMIT_ACTION, "09");

    HoodieInstant instant6Requested = createInstantV1(REQUESTED, HoodieTimeline.COMMIT_ACTION, "11");
    HoodieInstant instant6Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMMIT_ACTION, "11");

    List<HoodieInstant> instants = Arrays.asList(instant1Requested, instant1Inflight, instant2Requested, instant2Inflight,
        instant2Complete, instant3Requested, instant3Inflight, instant3Complete, instant4Requested, instant4Inflight,
        instant4Complete, instant5Requested, instant5Inflight, instant5Complete, instant6Requested, instant6Inflight);

    List<IndexedRecord> records = new ArrayList<>();
    StoragePath archiveFilePath = ArchivedTimelineV1.getArchiveLogPath(metaClient.getArchivePath());
    HoodieLogFormat.Writer writer;

    // Write archive commit 1
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant1Requested));
    records.add(createArchivedMetaWrapper(instant1Inflight));
    records.add(createArchivedMetaWrapper(instant2Requested));
    writeArchiveLog(writer, records);

    records.add(createArchivedMetaWrapper(instant2Inflight));
    records.add(createArchivedMetaWrapper(instant2Complete));
    records.add(createArchivedMetaWrapper(instant3Requested));
    writeArchiveLog(writer, records);
    writer.close();

    // Write archive commit 2
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant3Inflight));
    writeArchiveLog(writer, records);

    records.add(createArchivedMetaWrapper(instant3Complete));
    records.add(createArchivedMetaWrapper(instant4Requested));
    records.add(createArchivedMetaWrapper(instant4Inflight));
    records.add(createArchivedMetaWrapper(instant4Complete));
    records.add(createArchivedMetaWrapper(instant5Requested));
    records.add(createArchivedMetaWrapper(instant5Inflight));
    records.add(createArchivedMetaWrapper(instant5Complete));
    writeArchiveLog(writer, records);
    writer.close();

    // Write archive commit 3
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant6Requested));
    records.add(createArchivedMetaWrapper(instant6Inflight));
    writeArchiveLog(writer, records);
    writer.close();

    return instants;
  }

  // A separate method for creating additional instants of more actions - for additional tests on top of existing ones
  private List<HoodieInstant> createAdditionalInstants() throws Exception {
    HoodieInstant instant7Requested = createInstantV1(REQUESTED, HoodieTimeline.CLEAN_ACTION, "15");
    HoodieInstant instant7Inflight = createInstantV1(INFLIGHT, HoodieTimeline.CLEAN_ACTION, "15");
    HoodieInstant instant7Complete = createInstantV1(COMPLETED, HoodieTimeline.CLEAN_ACTION, "15");

    HoodieInstant instant8Requested = createInstantV1(REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "17");
    HoodieInstant instant8Inflight = createInstantV1(INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "17");
    HoodieInstant instant8Complete = createInstantV1(COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "17");

    HoodieInstant instant9Requested = createInstantV1(REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "19");
    HoodieInstant instant9Inflight = createInstantV1(INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, "19");
    HoodieInstant instant9Complete = createInstantV1(COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "19");

    HoodieInstant instant10Requested = createInstantV1(REQUESTED, HoodieTimeline.COMPACTION_ACTION, "21");
    HoodieInstant instant10Inflight = createInstantV1(INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "21");
    HoodieInstant instant10Complete = createInstantV1(COMPLETED, HoodieTimeline.COMPACTION_ACTION, "21");

    List<HoodieInstant> instantsInCommit1 = Arrays.asList(instant7Requested, instant7Inflight, instant7Complete,
        instant8Requested, instant8Inflight, instant8Complete);
    List<HoodieInstant> instantsInCommit2 = Arrays.asList(instant9Requested, instant9Inflight, instant9Complete,
        instant10Requested, instant10Inflight, instant10Complete);

    List<IndexedRecord> records = new ArrayList<>();
    StoragePath archiveFilePath = ArchivedTimelineV1.getArchiveLogPath(metaClient.getArchivePath());
    HoodieLogFormat.Writer writer;

    // Write archive commit 1
    writer = buildWriter(archiveFilePath);
    for (HoodieInstant instant : instantsInCommit1) {
      records.add(createArchivedMetaWrapper(instant));
    }
    writeArchiveLog(writer, records);
    writer.close();

    // Write archive commit 2
    writer = buildWriter(archiveFilePath);
    for (HoodieInstant instant : instantsInCommit2) {
      records.add(createArchivedMetaWrapper(instant));
    }
    writeArchiveLog(writer, records);
    writer.close();

    return Stream.concat(instantsInCommit1.stream(), instantsInCommit2.stream()).collect(Collectors.toList());
  }

  /**
   * Get list of archived log file paths.
   */
  private List<String> getArchiveLogFilePaths() throws IOException {
    return metaClient.getStorage().listDirectEntries(new StoragePath(metaClient.getArchivePath() + "/"), new StoragePathFilter() {
          @Override
          public boolean accept(StoragePath path) {
            return path.getName().contains("commits_.archive");
          }
        }).stream().map(storagePathInfo -> storagePathInfo.getPath().toString()).sorted().collect(Collectors.toList());
  }

  // Seems redundant to MetadataConversionUtils but MetadataConversionUtils cannot be imported as it is in
  // hudi-client module. So are defining the method here with simplified logic for testing.
  private HoodieArchivedMetaEntry createArchivedMetaWrapper(HoodieInstant hoodieInstant) throws IOException {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.requestedTime());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        // Hoodie commit metadata is required for archived timeline to load instants to memory
        archivedMetaWrapper.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build());
        break;
      case HoodieTimeline.COMPACTION_ACTION:
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        archivedMetaWrapper.setHoodieCompactionPlan(HoodieCompactionPlan.newBuilder().build());
        break;
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieReplaceCommitMetadata(HoodieReplaceCommitMetadata.newBuilder().build());
        } else if (hoodieInstant.isInflight()) {
          archivedMetaWrapper.setHoodieInflightReplaceMetadata(convertCommitMetadata(new org.apache.hudi.common.model.HoodieCommitMetadata(false)));
        } else {
          archivedMetaWrapper.setHoodieRequestedReplaceMetadata(new HoodieRequestedReplaceMetadata());
        }
        break;
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name());
        archivedMetaWrapper.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build());
        break;
      case HoodieTimeline.CLEAN_ACTION:
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieCleanMetadata(HoodieCleanMetadata.newBuilder()
              .setVersion(1)
              .setTimeTakenInMillis(100)
              .setTotalFilesDeleted(1)
              .setStartCleanTime("01")
              .setEarliestCommitToRetain("01")
              .setLastCompletedCommitTimestamp("")
              .setPartitionMetadata(new HashMap<>()).build());
        } else {
          // Dummy data for testing
          archivedMetaWrapper.setHoodieCleanerPlan(HoodieCleanerPlan.newBuilder()
              .setEarliestInstantToRetainBuilder(HoodieActionInstant.newBuilder()
                  .setAction("commit")
                  .setTimestamp("01")
                  .setState("COMPLETED"))
              .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
              .setFilesToBeDeletedPerPartition(new HashMap<>())
              .setVersion(CleanPlanV2MigrationHandler.VERSION)
              .build());
        }
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      case HoodieTimeline.ROLLBACK_ACTION:
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieRollbackMetadata(HoodieRollbackMetadata.newBuilder()
              .setVersion(1)
              .setStartRollbackTime("16")
              .setTotalFilesDeleted(1)
              .setTimeTakenInMillis(1000)
              .setCommitsRollback(Collections.singletonList("15"))
              .setPartitionMetadata(Collections.emptyMap())
              .setInstantsRollback(Collections.emptyList())
              .build());
        }
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        archivedMetaWrapper.setActionType(ActionType.logcompaction.name());
        HoodieCompactionPlan plan = CompactionUtils.getLogCompactionPlan(metaClient, hoodieInstant.requestedTime());
        archivedMetaWrapper.setHoodieCompactionPlan(plan);
        break;
      default:
        break;
    }
    return archivedMetaWrapper;
  }

  private Writer buildWriter(StoragePath archiveFilePath) throws IOException {
    return HoodieLogFormat.newWriterBuilder().onParentPath(archiveFilePath.getParent())
        .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
        .withStorage(metaClient.getStorage()).withInstantTime("").build();
  }

  private void writeArchiveLog(Writer writer, List<IndexedRecord> records) throws Exception {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieArchivedMetaEntry.getClassSchema().toString());
    final String keyField = metaClient.getTableConfig().getRecordKeyFieldProp();
    List<HoodieRecord> indexRecords = records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
    HoodieAvroDataBlock block = new HoodieAvroDataBlock(indexRecords, header, keyField);
    writer.appendBlock(block);
    records.clear();
  }

  private static HoodieInstant createInstantV1(HoodieInstant.State state,
                                               String action,
                                               String requestedTime) {
    return INSTANT_GENERATOR_V1.createNewInstant(state, action, requestedTime);
  }

  private static org.apache.hudi.avro.model.HoodieCommitMetadata convertCommitMetadata(
      HoodieCommitMetadata hoodieCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    // Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData =
        mapper.convertValue(hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    if (hoodieCommitMetadata.getCompacted()) {
      avroMetaData.setOperationType(WriteOperationType.COMPACT.name());
    }
    // Do not archive Rolling Stats, cannot set to null since AVRO will throw null pointer
    avroMetaData.getExtraMetadata().put(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, "");
    return avroMetaData;
  }
}
