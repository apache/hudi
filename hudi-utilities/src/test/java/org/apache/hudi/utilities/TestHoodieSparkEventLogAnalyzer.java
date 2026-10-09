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

package org.apache.hudi.utilities;

import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.eventlog.EventLogAnalysis;
import org.apache.hudi.utilities.eventlog.EventLogParser;
import org.apache.hudi.utilities.eventlog.HudiOperation;
import org.apache.hudi.utilities.eventlog.HudiOperationResolver;
import org.apache.hudi.utilities.eventlog.HudiPhase;
import org.apache.hudi.utilities.eventlog.HudiPhaseResolver;
import org.apache.hudi.utilities.eventlog.StageStats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSparkEventLogAnalyzer} and the event-log parsing behind it.
 *
 * <p>Fixtures are synthetic event logs written as newline-delimited JSON to a temp file, which is
 * what a real Spark event log is. No SparkSession is needed or created.
 */
class TestHoodieSparkEventLogAnalyzer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  // -------------------------------------------------------------------------------------------
  // Phase derivation
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      // Current encoding: "<module>:<activity>", the activity needing a prefix match past the value.
      // The five write-operation buckets.
      "StreamSync:Fetching next batch: my_table                                  | SOURCE_READ_AND_TRANSFORM",
      "StreamSync:Checking if input is empty: my_table                           | SOURCE_READ_AND_TRANSFORM",
      "HoodieWriteHelper:Tagging: my_table                                       | DEDUP_AND_INDEX_TAGGING",
      "HoodieBloomIndex:Compute all comparisons needed between records and files: t | DEDUP_AND_INDEX_TAGGING",
      "BaseSparkCommitActionExecutor:Doing partition and writing data: my_table  | DATA_TABLE_WRITE",
      "SparkRDDWriteClient:Committing stats: my_table                            | DATA_TABLE_WRITE",
      // "Building workload profile:" is the job under which dedup, index tagging and profiling all
      // run, so it belongs with the tagging bucket rather than with the write.
      "SparkUpsertCommitActionExecutor:Building workload profile:my_table        | DEDUP_AND_INDEX_TAGGING",
      // Small-file probing folds into the write bucket; the subPhase keeps it visible.
      "UpsertPartitioner:Getting small files from partitions: my_table           | DATA_TABLE_WRITE",
      "HoodieTable:Delete all partially written files: my_table                  | MARKER_RECONCILIATION",
      "BaseHoodieClient:Committing to metadata table: my_table                   | METADATA_TABLE_WRITE",
      // Table services share PLANNING and EXECUTION; only the operation says which service.
      "ScheduleCompactionActionExecutor:Compaction: generating compaction plan   | PLANNING",
      "BaseHoodieCompactionPlanGenerator:Looking for files to compact: my_table  | PLANNING",
      "CleanPlanActionExecutor:Obtaining list of partitions to be cleaned: t     | PLANNING",
      "HoodieCompactor:Compacting file slices: my_table                          | EXECUTION",
      "BaseCommitActionExecutor:Clustering records for my_table                  | EXECUTION",
      "CleanActionExecutor:Perform cleaning of table: my_table                   | EXECUTION",
      "RollbackHelper:Perform rollback actions: my_table                         | EXECUTION",
      "SavepointActionExecutor:Collecting latest files for savepoint 20240101    | PLANNING",
      "SparkBootstrapCommitActionExecutor:Run metadata-only bootstrap operation: t | EXECUTION",
      "TimelineArchiverV2:Delete archived instants: my_table                     | ARCHIVAL",
      // Legacy encoding (before HUDI-8596): the bare activity, module was in spark.jobGroup.id.
      "Tagging: my_table                                                         | DEDUP_AND_INDEX_TAGGING",
      "Getting small files from partitions: my_table                             | DATA_TABLE_WRITE",
      // Activity strings that embed a count rather than ending in a separator.
      "SparkHoodieBackedTableMetadataWriter:Listing 12 partitions from filesystem | METADATA_TABLE_WRITE",
      "HoodieBackedTableMetadataWriter:Creating 4 file groups for partition files | METADATA_TABLE_WRITE",
      // Longest-prefix wins: this starts with "Creating " but is rollback planning, not an MDT write.
      "ListingBasedRollbackStrategy:Creating Listing Rollback Plan: my_table     | PLANNING",
      // Insert-overwrite file-id resolution is a sub-step of the write, not a peer bucket.
      "SparkInsertOverwriteTableCommitActionExecutor:Getting ExistingFileIds of all partitions | DATA_TABLE_WRITE",
      "SparkDeletePartitionCommitActionExecutor:Gather all file ids from all deleting partitions. | DATA_TABLE_WRITE"})
  void givenKnownJobDescription_whenResolvingPhase_thenMapsToExpectedPhase(String description, String expectedPhase) {
    assertEquals(HudiPhase.valueOf(expectedPhase.trim()), HudiPhaseResolver.resolve(description.trim()),
        "job description should map to its Hudi phase: " + description.trim());
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "Listing leaf files and directories for 200 paths       |",
      "count at MyApplication.scala:42                        |",
      "SomeOtherEngine:Doing something entirely unrelated     |",
      "                                                       |"})
  void givenUnrecognizedJobDescription_whenResolvingPhase_thenReturnsUnknown(String description) {
    assertEquals(HudiPhase.UNKNOWN, HudiPhaseResolver.resolve(description == null ? null : description.trim()),
        "an unrecognized description must not be guessed at");
  }

  @Test
  void givenNullJobDescription_whenResolvingPhase_thenReturnsUnknown() {
    assertEquals(HudiPhase.UNKNOWN, HudiPhaseResolver.resolve(null));
  }

  @Test
  void givenCurrentEncoding_whenResolvingModule_thenExtractsModuleFromDescription() {
    assertEquals("HoodieWriteHelper", HudiPhaseResolver.resolveModule("HoodieWriteHelper:Tagging: my_table", null));
  }

  @Test
  void givenLegacyEncoding_whenResolvingModule_thenTakesModuleFromJobGroup() {
    assertEquals("HoodieWriteHelper", HudiPhaseResolver.resolveModule("Tagging: my_table", "HoodieWriteHelper"));
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "HoodieWriteHelper:Tagging: my_table                               | partitionReplace | false",
      "SparkInsertOverwriteTableCommitActionExecutor:Getting ExistingFileIds of all partitions | partitionReplace | true",
      "FlinkDeletePartitionCommitActionExecutor:Gather all file ids from all deleting partitions. | partitionReplace | true"})
  void givenInsertOverwriteDescription_whenResolving_thenRecordsPartitionReplaceSubPhase(
      String description, String expectedSubPhase, boolean shouldMatch) {
    HudiPhaseResolver.Match match = HudiPhaseResolver.resolveMatch(description.trim());
    if (shouldMatch) {
      assertEquals(HudiPhase.DATA_TABLE_WRITE, match.getPhase(),
          "partition replacement is a sub-step of the write, not a bucket of its own");
      assertEquals(expectedSubPhase.trim(), match.getSubPhase());
    } else {
      assertNotEquals(expectedSubPhase.trim(), match.getSubPhase());
    }
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SparkUpsertCommitActionExecutor:Building workload profile:my_table        | workloadProfile",
      "UpsertPartitioner:Getting small files from partitions: my_table           | smallFileProbe",
      "HoodieBloomIndex:Compute all comparisons needed between records and files: t | bloomComparisonFanout",
      "ListingBasedRollbackStrategy:Creating Listing Rollback Plan: my_table     | rollbackInstantListing",
      "ScheduleCompactionActionExecutor:Compaction: generating compaction plan   | compactionPlan",
      "HoodieCompactor:Compacting file slices: my_table                          | compaction",
      "BaseCommitActionExecutor:Clustering records for my_table                  | clustering",
      "CleanActionExecutor:Perform cleaning of table: my_table                   | cleaning"})
  void givenCoarseBucket_whenResolving_thenFinerLabelSurvivesAsSubPhase(String description, String expectedSubPhase) {
    // The buckets are deliberately few, so the finer label has to be preserved somewhere for a
    // reader who needs it; that somewhere is the sub-phase.
    assertEquals(expectedSubPhase.trim(), HudiPhaseResolver.resolveMatch(description.trim()).getSubPhase());
  }

  @Test
  void givenRollbackInstantListing_whenResolving_thenFoldedIntoPlanningButStillIdentifiable() {
    // Identifying which instants to roll back is often the expensive part of a rollback. It is not
    // separable into its own bucket, but the sub-phase keeps it visible.
    HudiPhaseResolver.Match match =
        HudiPhaseResolver.resolveMatch("ListingBasedRollbackStrategy:Creating Listing Rollback Plan: t");

    assertEquals(HudiPhase.PLANNING, match.getPhase());
    assertEquals("rollbackInstantListing", match.getSubPhase());
  }

  // -------------------------------------------------------------------------------------------
  // Resolution from the stage name, for Hudi paths that set no job description at all
  // -------------------------------------------------------------------------------------------

  @Test
  void givenRealRecordIndexStages_whenParsing_thenTaggingIsVisibleWithTheLookupSubPhase() throws IOException {
    // given the four stage/description pairs observed verbatim in a real RECORD_INDEX run. Hudi sets
    // "Building workload profile:" once and the whole lazy chain -- dedup, tagging, profiling --
    // executes beneath it, so the description is correct but coarser than the component boundary.
    String description = "SparkUpsertCommitActionExecutor:Building workload profile:t1";
    String lookupStage = "mapPartitionsToPair at SparkMetadataTableGlobalRecordLevelIndex.java:133";
    String keyByStage = "keyBy at SparkMetadataTableGlobalRecordLevelIndex.java:124";

    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, description, null, 10, 11, 12, 13));
    events.add(stageSubmitted(10, 0, lookupStage, 1, 1000L));
    events.add(stageCompleted(10, 0, lookupStage, 1, 1000L, 2944L, null));
    events.add(stageSubmitted(11, 0, keyByStage, 1, 2944L));
    events.add(stageCompleted(11, 0, keyByStage, 1, 2944L, 3101L, null));
    events.add(stageSubmitted(12, 0, lookupStage, 1, 3101L));
    events.add(stageCompleted(12, 0, lookupStage, 1, 3101L, 4477L, null));
    events.add(stageSubmitted(13, 0, keyByStage, 1, 4477L));
    events.add(stageCompleted(13, 0, keyByStage, 1, 4477L, 4611L, null));
    Path log = writeLog("real-rli-stages", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then all four land in the tagging bucket, named for the component that actually ran
    for (StageStats stage : analysis.getStages()) {
      assertEquals(HudiPhase.DEDUP_AND_INDEX_TAGGING, stage.getPhase(),
          "the workload-profile job is where dedup and index tagging run");
      assertEquals("recordIndexLookup", stage.getSubPhase(),
          "the stage name refines which component ran inside that job");
      assertEquals(HudiOperation.WRITE_OPERATION, stage.getOperation());
    }
    assertEquals(1, analysis.getPhaseSummaries().size(), "every stage in this log is attributable");
  }

  @Test
  void givenWorkloadProfileWithNoIndexMarker_whenParsing_thenKeepsTheProfileSubPhase() throws IOException {
    // given the same job description, but a stage name that names no index component
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "SparkUpsertCommitActionExecutor:Building workload profile:t1", null, 10));
    events.add(stageSubmitted(10, 0, "countByKey at BaseSparkCommitActionExecutor.java:284", 1, 1000L));
    events.add(stageCompleted(10, 0, "countByKey at BaseSparkCommitActionExecutor.java:284", 1, 1000L, 2000L, null));
    Path log = writeLog("workload-profile-plain", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then the phase is unchanged and the sub-phase falls back to what the description implied
    assertEquals(HudiPhase.DEDUP_AND_INDEX_TAGGING, stage.getPhase());
    assertEquals("workloadProfile", stage.getSubPhase());
  }

  @Test
  void givenIndexStageNameUnderADifferentPhase_whenParsing_thenDescriptionStillDecidesThePhase() throws IOException {
    // given a compaction job whose stage name happens to mention an index class
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieCompactor:Compacting file slices: t", null, 10));
    events.add(stageSubmitted(10, 0, "x at SparkMetadataTableGlobalRecordLevelIndex.java:1", 1, 1000L));
    events.add(stageCompleted(10, 0, "x at SparkMetadataTableGlobalRecordLevelIndex.java:1", 1, 1000L, 2000L, null));
    Path log = writeLog("description-decides-phase", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then the stage name never changes the phase, and refinement does not apply outside tagging
    assertEquals(HudiPhase.EXECUTION, stage.getPhase());
    assertEquals("compaction", stage.getSubPhase());
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "mapPartitionsToPair at SparkMetadataTableGlobalRecordLevelIndex.java:133 | recordIndexLookup",
      "keyBy at SparkMetadataTableGlobalRecordLevelIndex.java:124               | recordIndexLookup",
      "mapPartitions at SparkMetadataTableRecordLevelIndex.java:88              | recordIndexLookup",
      "mapPartitionsWithIndex at SparkHoodieBloomIndexHelper.java:173           | bloomIndexLookup",
      "countByKey at BaseSparkCommitActionExecutor.java:284                     |"})
  void givenStageName_whenRefiningTaggingSubPhase_thenNamesTheComponentThatRan(String stageName, String expected) {
    assertEquals(expected == null ? null : expected.trim(),
        HudiPhaseResolver.refineSubPhase(HudiPhase.DEDUP_AND_INDEX_TAGGING, stageName.trim()));
  }

  @Test
  void givenNonTaggingPhase_whenRefiningSubPhase_thenDeclinesToRefine() {
    // refinement is scoped to the one bucket where a job description is known to cover several
    // components; elsewhere the description's own sub-phase stands.
    assertNull(HudiPhaseResolver.refineSubPhase(HudiPhase.DATA_TABLE_WRITE,
        "mapPartitionsToPair at SparkMetadataTableGlobalRecordLevelIndex.java:133"));
  }

  @Test
  void givenStreamingMetadataStageWithNoDescription_whenResolving_thenFallsBackToStageName() {
    // the fallback path still exists for genuinely untagged work, and is weaker than a description
    HudiPhaseResolver.Match match = HudiPhaseResolver.resolveFromStageName(
        "mapToPair at SparkStreamingMetadataWriteHandler.java:61");

    assertEquals(HudiPhase.UNKNOWN, match.getPhase());
    assertEquals("streamingMetadataWrite", match.getSubPhase());
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "FSUtils:Parallel listing paths file:/data/tbl/.hoodie/metadata/.hoodie/.temp/001/files | METADATA_TABLE_WRITE",
      "FSUtils:Parallel listing paths file:/data/tbl/.hoodie/.temp/20260101/MARKERS0          | UNKNOWN"})
  void givenParallelListing_whenResolving_thenOnlyMetadataPathsAreAttributed(String description, String expectedPhase) {
    // The listed paths are the only thing saying what a parallel listing is for. A data-table
    // listing stays UNKNOWN rather than being guessed at.
    assertEquals(HudiPhase.valueOf(expectedPhase.trim()), HudiPhaseResolver.resolve(description.trim()));
  }

  @Test
  void givenMetadataParallelListing_whenResolving_thenCarriesListingSubPhaseAndIndexingOperation() throws IOException {
    // given
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L,
        "FSUtils:Parallel listing paths file:/data/tbl/.hoodie/metadata/.hoodie/.temp/001/files", null, 10));
    events.add(stageSubmitted(10, 0, "listing", 1, 1000L));
    events.add(stageCompleted(10, 0, "listing", 1, 1000L, 2000L, null));
    Path log = writeLog("metadata-listing", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(HudiPhase.METADATA_TABLE_WRITE, stage.getPhase());
    assertEquals("metadataListing", stage.getSubPhase());
    assertEquals(HudiOperation.INDEXING, stage.getOperation());
  }

  // -------------------------------------------------------------------------------------------
  // The streaming metadata-write limitation
  // -------------------------------------------------------------------------------------------

  @Test
  void givenStreamingMetadataWriteStages_whenParsing_thenKeepsThemUnknownButLabelsThem() throws IOException {
    // given a run dominated by the untagged streaming metadata-write path
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 2000L, null));
    events.add(jobStart(1, 2000L, null, null, 11));
    events.add(stageSubmitted(11, 0, "mapToPair at SparkStreamingMetadataWriteHandler.java:61", 1, 2000L));
    events.add(stageCompleted(11, 0, "mapToPair at SparkStreamingMetadataWriteHandler.java:61", 1, 2000L, 12000L, null));
    Path log = writeLog("streaming-metadata-write", events);

    // when
    EventLogAnalysis analysis = parse(log);
    StageStats streamingStage = analysis.getStages().get(1);

    // then the phase stays honest, but a reader can see what the unattributed work is
    assertEquals(HudiPhase.UNKNOWN, streamingStage.getPhase(),
        "this path is genuinely unattributable; labelling it would be a false claim");
    assertEquals("streamingMetadataWrite", streamingStage.getSubPhase());

    String caveat = analysis.getCaveats().get(0);
    assertTrue(caveat.contains("hoodie.metadata.streaming.write.enabled"),
        "the caveat must name the config so a user can confirm it: " + caveat);
    assertTrue(caveat.contains("understate"), caveat);
    // 10s of the 11s of stage wall clock is on the untagged path.
    assertTrue(caveat.contains("91%"), "the caveat should quantify the unattributed share: " + caveat);
  }

  @Test
  void givenSmallUnattributedShare_whenParsing_thenOmitsTheStreamingCaveat() throws IOException {
    // given the same path present but trivial, which is not worth a prominent warning
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 11000L, null));
    events.add(jobStart(1, 11000L, null, null, 11));
    events.add(stageSubmitted(11, 0, "mapToPair at SparkStreamingMetadataWriteHandler.java:61", 1, 11000L));
    events.add(stageCompleted(11, 0, "mapToPair at SparkStreamingMetadataWriteHandler.java:61", 1, 11000L, 11100L, null));
    Path log = writeLog("streaming-metadata-small", events);

    // when
    List<String> caveats = parse(log).getCaveats();

    // then
    assertFalse(caveats.get(0).contains("hoodie.metadata.streaming.write.enabled"),
        "a trivial unattributed share should not lead with a config warning: " + caveats.get(0));
  }

  @Test
  void givenEmptyColonJobDescription_whenResolving_thenStaysUnknownAndIsMentionedInCaveats() throws IOException {
    // given the literal ":" description Hudi emits when it clears the job status
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, ":", null, 10));
    events.add(stageSubmitted(10, 0, "some stage", 1, 1000L));
    events.add(stageCompleted(10, 0, "some stage", 1, 1000L, 2000L, null));
    Path log = writeLog("empty-colon", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then it is not mapped to anything, but it is explained rather than silently lumped in
    assertEquals(HudiPhase.UNKNOWN, analysis.getStages().get(0).getPhase());
    assertTrue(analysis.getCaveats().stream().anyMatch(caveat -> caveat.contains("\":\"")),
        "the empty-colon description is a known untagged case and should be named: "
            + analysis.getCaveats());
  }

  // -------------------------------------------------------------------------------------------
  // Operation derivation
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "HoodieCompactor                                  | COMPACTION",
      "ScheduleCompactionActionExecutor                 | COMPACTION",
      "RunCompactionActionExecutor                      | COMPACTION",
      "SparkInsertOverwriteTableCommitActionExecutor    | WRITE_OPERATION",
      "SparkUpsertCommitActionExecutor                  | WRITE_OPERATION",
      "HoodieWriteHelper                                | WRITE_OPERATION",
      "SparkRDDWriteClient                              | WRITE_OPERATION",
      "UpsertPartitioner                                | WRITE_OPERATION",
      "CleanActionExecutor                              | CLEANING",
      "CleanPlanActionExecutor                          | CLEANING",
      "BaseRestoreActionExecutor                        | RESTORE",
      "RollbackHelper                                   | ROLLBACK",
      "ListingBasedRollbackStrategy                     | ROLLBACK",
      "SavepointActionExecutor                          | SAVEPOINT",
      "SparkBootstrapCommitActionExecutor               | BOOTSTRAP",
      "TimelineArchiverV2                               | ARCHIVAL",
      "StreamSync                                       | STREAMER_SYNC",
      "BaseRecordIndexer                                | INDEXING",
      "SparkHoodieBackedTableMetadataWriterTableVersionSix | INDEXING",
      "SparkExecuteClusteringCommitActionExecutor       | CLUSTERING"})
  void givenModuleName_whenResolvingOperation_thenMapsToExpectedOperationFromTheModule(
      String module, String expectedOperation) {
    HudiOperationResolver.Resolution resolution =
        HudiOperationResolver.resolve(module.trim(), Collections.emptyList());

    assertEquals(HudiOperation.valueOf(expectedOperation.trim()), resolution.getOperation());
    assertEquals(HudiOperation.Source.MODULE, resolution.getSource(),
        "the module is the stronger signal and should be reported as the source");
  }

  @Test
  void givenNoModule_whenResolvingOperationFromWriteBuckets_thenInfersWriteOperation() {
    // given the buckets of a write pipeline, with no module to go on
    List<HudiPhase> phases = java.util.Arrays.asList(
        HudiPhase.DEDUP_AND_INDEX_TAGGING, HudiPhase.DATA_TABLE_WRITE, HudiPhase.MARKER_RECONCILIATION);

    // when
    HudiOperationResolver.Resolution resolution = HudiOperationResolver.resolve(null, phases);

    // then
    assertEquals(HudiOperation.WRITE_OPERATION, resolution.getOperation());
    assertEquals(HudiOperation.Source.SEQUENCE, resolution.getSource());
  }

  @Test
  void givenSharedPlanningBucket_whenResolvingOperation_thenSubPhaseSaysWhichTableService() {
    // given PLANNING, which on its own says nothing about which service is running
    List<HudiPhase> planning = Collections.singletonList(HudiPhase.PLANNING);

    // then the sub-phase is what disambiguates it
    assertEquals(HudiOperation.COMPACTION, HudiOperationResolver.resolve(
        null, planning, Collections.singletonList("compactionPlan")).getOperation());
    assertEquals(HudiOperation.CLEANING, HudiOperationResolver.resolve(
        null, planning, Collections.singletonList("cleanPartitionScan")).getOperation());
    assertEquals(HudiOperation.ROLLBACK, HudiOperationResolver.resolve(
        null, planning, Collections.singletonList("rollbackInstantListing")).getOperation());
    assertEquals(HudiOperation.CLUSTERING, HudiOperationResolver.resolve(
        null, Collections.singletonList(HudiPhase.EXECUTION),
        Collections.singletonList("clustering")).getOperation());
  }

  @Test
  void givenSharedBucketWithNoSubPhase_whenResolvingOperation_thenDoesNotGuessTheService() {
    // given EXECUTION with nothing to say which table service it belongs to
    HudiOperationResolver.Resolution resolution = HudiOperationResolver.resolve(
        null, Collections.singletonList(HudiPhase.EXECUTION), Collections.singletonList(null));

    // then
    assertEquals(HudiOperation.UNKNOWN, resolution.getOperation(),
        "PLANNING and EXECUTION are shared across table services, so neither may be guessed at");
    assertEquals(HudiOperation.Source.NONE, resolution.getSource());
  }

  @Test
  void givenNeitherModuleNorPhases_whenResolvingOperation_thenUnknownWithSourceNone() {
    HudiOperationResolver.Resolution resolution =
        HudiOperationResolver.resolve(null, Collections.singletonList(HudiPhase.UNKNOWN));

    assertEquals(HudiOperation.UNKNOWN, resolution.getOperation(), "an unresolved operation is not guessed");
    assertEquals(HudiOperation.Source.NONE, resolution.getSource());
    assertEquals("none", resolution.getSource().toJson());
  }

  @Test
  void givenUnrecognizedModule_whenResolvingOperation_thenFallsBackToPhaseSequence() {
    HudiOperationResolver.Resolution resolution = HudiOperationResolver.resolve(
        "SomeFrameworkNobodyHasHeardOf", Collections.singletonList(HudiPhase.EXECUTION),
        Collections.singletonList("compaction"));

    assertEquals(HudiOperation.COMPACTION, resolution.getOperation());
    assertEquals(HudiOperation.Source.SEQUENCE, resolution.getSource(),
        "the module did not resolve, so the sequence is the signal that did");
  }

  @Test
  void givenSingleRollback_whenParsing_thenReportsRollbackNotRestore() throws IOException {
    // given one rollback, which is what a single failed write produces
    List<String> events = new ArrayList<>();
    events.addAll(rollbackJob(0, 10, 1000L, 1200L));
    Path log = writeLog("single-rollback", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(HudiOperation.ROLLBACK, stage.getOperation(),
        "one rollback is a rollback; promoting it to a restore would be a guess");
  }

  @Test
  void givenManyRepeatedRollbacks_whenParsing_thenPromotesThemToRestore() throws IOException {
    // given four rollback job groups in one application, which is what a restore looks like:
    // BaseRestoreActionExecutor loops over the instants and calls the ordinary rollback path for
    // each, emitting no marker of its own.
    List<String> events = new ArrayList<>();
    for (int jobId = 0; jobId < 4; jobId++) {
      events.addAll(rollbackJob(jobId, 10 + jobId, 1000L + jobId * 500L, 1400L + jobId * 500L));
    }
    Path log = writeLog("restore", events);

    // when
    List<StageStats> stages = parse(log).getStages();

    // then
    assertEquals(4, stages.size());
    for (StageStats stage : stages) {
      assertEquals(HudiOperation.RESTORE, stage.getOperation(),
          "repeated rollbacks within one application are a restore");
      assertEquals(HudiOperation.Source.SEQUENCE, stage.getOperationSource());
    }
  }

  @Test
  void givenLargeSourceReadStage_whenParsing_thenReportsItAsACompositeOfUntaggedWork() throws IOException {
    // given a source-read stage dominating the run, which in reality also carries the transformation
    // and record creation that Hudi never tags
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "StreamSync:Checking if input is empty: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "read", 1, 1000L));
    events.add(stageCompleted(10, 0, "read", 1, 1000L, 9000L, null));
    events.add(jobStart(1, 9000L, "HoodieWriteHelper:Tagging: my_table", null, 11));
    events.add(stageSubmitted(11, 0, "tag", 1, 9000L));
    events.add(stageCompleted(11, 0, "tag", 1, 9000L, 9500L, null));
    Path log = writeLog("source-read-composite", events);

    // when
    StageStats sourceRead = parse(log).getStages().get(0);

    // then
    assertEquals(HudiPhase.SOURCE_READ_AND_TRANSFORM, sourceRead.getPhase(),
        "the bucket name already states what it covers");
    assertEquals(
        java.util.Arrays.asList("source read", "transformation", "record creation"),
        sourceRead.getBundledSteps(),
        "a dominant source read must declare the untagged work it absorbs");
  }

  @Test
  void givenSmallSourceReadStage_whenParsing_thenOmitsTheCompositeNote() throws IOException {
    // given a source read that is a trivial share of the run
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "StreamSync:Fetching next batch: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "read", 1, 1000L));
    events.add(stageCompleted(10, 0, "read", 1, 1000L, 1050L, null));
    events.add(jobStart(1, 1050L, "HoodieWriteHelper:Tagging: my_table", null, 11));
    events.add(stageSubmitted(11, 0, "tag", 1, 1050L));
    events.add(stageCompleted(11, 0, "tag", 1, 1050L, 11050L, null));
    Path log = writeLog("source-read-small", events);

    // when
    StageStats sourceRead = parse(log).getStages().get(0);

    // then
    assertEquals(HudiPhase.SOURCE_READ_AND_TRANSFORM, sourceRead.getPhase());
    assertTrue(sourceRead.getBundledSteps().isEmpty(),
        "the composite note only earns its place when the number is big enough to act on");
  }

  @Test
  void givenMixedOperations_whenParsing_thenOperationSummaryAccountsForEachSeparately() throws IOException {
    // given a write operation followed by a compaction in the same application
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "SparkUpsertCommitActionExecutor:Doing partition and writing data: t", null, 10));
    events.add(stageSubmitted(10, 0, "write", 1, 1000L));
    events.add(stageCompleted(10, 0, "write", 1, 1000L, 3000L, null));
    events.add(jobStart(1, 3000L, "HoodieCompactor:Compacting file slices: t", null, 11));
    events.add(stageSubmitted(11, 0, "compact", 1, 3000L));
    events.add(stageCompleted(11, 0, "compact", 1, 3000L, 4000L, null));
    Path log = writeLog("mixed-operations", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then the two stages carry different operations despite both doing write-shaped work
    assertEquals(HudiOperation.WRITE_OPERATION, analysis.getStages().get(0).getOperation());
    assertEquals(HudiOperation.COMPACTION, analysis.getStages().get(1).getOperation());

    List<EventLogAnalysis.GroupSummary> operations = analysis.getOperationSummaries();
    assertEquals(2, operations.size());
    assertEquals("WRITE_OPERATION", operations.get(0).getLabel(), "ordered by descending wall clock");
    assertEquals(2000L, operations.get(0).getStageWallClockMillis());
    assertEquals("COMPACTION", operations.get(1).getLabel());
  }

  // -------------------------------------------------------------------------------------------
  // Parsing and aggregation
  // -------------------------------------------------------------------------------------------

  @Test
  void givenSingleJobLog_whenParsing_thenReportsApplicationAndStage() throws IOException {
    // given
    List<String> events = new ArrayList<>();
    events.add(applicationStart("my-app", "app-1", 1000L));
    events.add(jobStart(0, 1100L, "HoodieWriteHelper:Tagging: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "mapPartitions at HoodieWriteHelper", 3, 1100L));
    events.add(taskEnd(10, 0, 1100L, 1200L, false, false, false, null, 90L, 10L, 0L, 0L));
    events.add(taskEnd(10, 0, 1100L, 1300L, false, false, false, null, 180L, 10L, 0L, 0L));
    events.add(taskEnd(10, 0, 1100L, 1500L, false, false, false, null, 380L, 10L, 0L, 0L));
    events.add(stageCompleted(10, 0, "mapPartitions at HoodieWriteHelper", 3, 1100L, 1500L, null));
    events.add(jobEnd(0, 1500L, "JobSucceeded"));
    events.add(applicationEnd(2000L));
    Path log = writeLog("single-job", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals("my-app", analysis.getAppName());
    assertEquals("app-1", analysis.getAppId());
    assertEquals(1000L, analysis.getDurationMillis());
    assertEquals(1, analysis.getStages().size());

    StageStats stage = analysis.getStages().get(0);
    assertEquals(10, stage.getStageId());
    assertEquals(HudiPhase.DEDUP_AND_INDEX_TAGGING, stage.getPhase());
    assertEquals("HoodieWriteHelper:Tagging: my_table", stage.getJobDescription());
    assertEquals(Integer.valueOf(0), stage.getJobId());
    assertEquals(3, stage.getNumTasksObserved());
    assertEquals(400L, stage.getDurationMillis());
    assertTrue(analysis.getWarnings().isEmpty(), "a well-formed log should produce no warnings");
  }

  @Test
  void givenStageWithSkewedTasks_whenParsing_thenComputesPercentilesAndSkewRatio() throws IOException {
    // given three tasks of 100ms, 200ms and 1000ms
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 3, 1000L));
    events.add(taskEnd(10, 0, 1000L, 1100L, false, false, false, null, 100L, 0L, 0L, 0L));
    events.add(taskEnd(10, 0, 1000L, 1200L, false, false, false, null, 200L, 0L, 0L, 0L));
    events.add(taskEnd(10, 0, 1000L, 2000L, false, false, false, null, 1000L, 0L, 0L, 0L));
    events.add(stageCompleted(10, 0, "tag", 3, 1000L, 2000L, null));
    Path log = writeLog("skew", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(200L, stage.getTaskDurationPercentile(50), "p50 of 100/200/1000 is the middle value");
    assertEquals(1000L, stage.getMaxTaskDurationMillis());
    assertEquals(5.0, stage.getSkewRatio(), 0.001, "skew ratio is max over p50");
    assertEquals(1300L, stage.getExecutorRunTimeMillis(), "executor run time sums across tasks");
  }

  @Test
  void givenStageWithNoTasks_whenComputingSkew_thenReportsUndefinedRatherThanDividingByZero() throws IOException {
    // given a stage that completed without any task ending
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 0, 1000L));
    events.add(stageCompleted(10, 0, "tag", 0, 1000L, 1200L, null));
    Path log = writeLog("no-tasks", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(-1L, stage.getTaskDurationPercentile(50));
    assertEquals(-1.0, stage.getSkewRatio(), 0.001, "an undefined skew ratio must be -1, not infinity");
    assertEquals(-1.0, stage.getGcFraction(), 0.001);
  }

  @Test
  void givenFailedAndSpeculativeTasks_whenParsing_thenAccountsForEachSeparately() throws IOException {
    // given one success, one speculative success, one OOM failure and one killed task
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "BaseSparkCommitActionExecutor:Doing partition and writing data: t", null, 10));
    events.add(stageSubmitted(10, 0, "write", 4, 1000L));
    events.add(taskEnd(10, 0, 1000L, 1100L, false, false, false, null, 100L, 0L, 0L, 0L));
    events.add(taskEnd(10, 0, 1000L, 1150L, false, false, true, null, 150L, 0L, 0L, 0L));
    events.add(taskEnd(10, 0, 1000L, 1200L, true, false, false, "java.lang.OutOfMemoryError", 200L, 0L, 0L, 0L));
    events.add(taskEnd(10, 0, 1000L, 1250L, false, true, false, null, 250L, 0L, 0L, 0L));
    events.add(stageCompleted(10, 0, "write", 4, 1000L, 1300L, null));
    Path log = writeLog("failures", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(HudiPhase.DATA_TABLE_WRITE, stage.getPhase());
    assertEquals(2, stage.getTasksSucceeded(), "the speculative task also succeeded");
    assertEquals(1, stage.getTasksFailed());
    assertEquals(1, stage.getTasksKilled());
    assertEquals(1, stage.getTasksSpeculative());
    assertEquals(4, stage.getNumTasksObserved());
    assertEquals(Integer.valueOf(1),
        stage.getFailureReasonCounts().get("ExceptionFailure: java.lang.OutOfMemoryError"),
        "the exception class must qualify the failure reason so distinct causes do not merge");
  }

  @Test
  void givenFailedStage_whenParsing_thenCapturesStageFailureReason() throws IOException {
    // given
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieCompactor:Compacting file slices: t", null, 10));
    events.add(stageSubmitted(10, 0, "compact", 1, 1000L));
    events.add(stageCompleted(10, 0, "compact", 1, 1000L, 1200L,
        "Job aborted due to stage failure: executor lost\n\tat org.apache.spark.Foo"));
    Path log = writeLog("failed-stage", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals("Job aborted due to stage failure: executor lost", stage.getFailureReason(),
        "only the first line of the failure reason is kept, so a report stays readable");
  }

  @Test
  void givenShuffleAndSpillMetrics_whenParsing_thenAggregatesAcrossTasks() throws IOException {
    // given two tasks that each shuffle and spill
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 2, 1000L));
    events.add(taskEnd(10, 0, 1000L, 1100L, false, false, false, null, 100L, 20L, 1024L, 2048L));
    events.add(taskEnd(10, 0, 1000L, 1200L, false, false, false, null, 200L, 30L, 512L, 4096L));
    events.add(stageCompleted(10, 0, "tag", 2, 1000L, 1200L, null));
    Path log = writeLog("shuffle", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(50L, stage.getJvmGcTimeMillis());
    assertEquals(50.0 / 300.0, stage.getGcFraction(), 0.001);
    assertEquals(1536L, stage.getMemoryBytesSpilled());
    assertEquals(6144L, stage.getDiskBytesSpilled());
    // Each task fixture writes 10x its memory-spill number: 1024*10 + 512*10.
    assertEquals(15360L, stage.getShuffleWriteBytes());
    // Shuffle read is remote (10) plus local (5), per task.
    assertEquals(30L, stage.getShuffleReadBytes());
    assertEquals(4L, stage.getShuffleFetchWaitTimeMillis());
  }

  @Test
  void givenMultipleJobs_whenParsing_thenCriticalPathSumsLongestStagePerJob() throws IOException {
    // given job 0 with a 400ms and a 100ms stage, and job 1 with a 300ms stage
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10, 11));
    events.add(stageSubmitted(10, 0, "a", 1, 1000L));
    events.add(stageCompleted(10, 0, "a", 1, 1000L, 1400L, null));
    events.add(stageSubmitted(11, 0, "b", 1, 1400L));
    events.add(stageCompleted(11, 0, "b", 1, 1400L, 1500L, null));
    events.add(jobEnd(0, 1500L, "JobSucceeded"));
    events.add(jobStart(1, 1500L, "HoodieCompactor:Compacting file slices: t", null, 12));
    events.add(stageSubmitted(12, 0, "c", 1, 1500L));
    events.add(stageCompleted(12, 0, "c", 1, 1500L, 1800L, null));
    events.add(jobEnd(1, 1800L, "JobSucceeded"));
    Path log = writeLog("critical-path", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals(700L, analysis.getCriticalPathMillis(), "400ms from job 0 plus 300ms from job 1");
    assertEquals(2, analysis.getJobCount());
    assertEquals(0, analysis.getFailedJobCount());
  }

  @Test
  void givenExecutorAddAndRemoveEvents_whenParsing_thenDerivesPeakConcurrency() throws IOException {
    // given two executors added, one removed, then one more added
    List<String> events = new ArrayList<>();
    events.add(executorAdded("1", 1000L));
    events.add(executorAdded("2", 1100L));
    events.add(executorRemoved("1", 1200L, "Container killed by YARN for exceeding memory limits"));
    events.add(executorAdded("3", 1300L));
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 1400L, null));
    Path log = writeLog("executors", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals(2, analysis.getPeakConcurrentExecutors(), "at most two executors were ever alive together");
    assertEquals("Container killed by YARN for exceeding memory limits",
        analysis.getExecutorRemovalReasons().get("1"));
  }

  @Test
  void givenUnrecognizedJobDescription_whenParsing_thenStageIsUnknownAndKeepsStageName() throws IOException {
    // given
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "SomeOtherFramework:doing its own thing", null, 10));
    events.add(stageSubmitted(10, 0, "collect at Unrelated.scala:17", 1, 1000L));
    events.add(stageCompleted(10, 0, "collect at Unrelated.scala:17", 1, 1000L, 1200L, null));
    Path log = writeLog("unknown-phase", events);

    // when
    StageStats stage = parse(log).getStages().get(0);

    // then
    assertEquals(HudiPhase.UNKNOWN, stage.getPhase());
    assertEquals("collect at Unrelated.scala:17", stage.getName(),
        "with no phase, the stage name is the only thing left to report");
  }

  @Test
  void givenStageRetry_whenParsing_thenKeepsAttemptsSeparate() throws IOException {
    // given one stage that failed and was retried
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 1200L, "FetchFailed"));
    events.add(stageSubmitted(10, 1, "tag", 1, 1200L));
    events.add(stageCompleted(10, 1, "tag", 1, 1200L, 1500L, null));
    Path log = writeLog("retry", events);

    // when
    List<StageStats> stages = parse(log).getStages();

    // then
    assertEquals(2, stages.size(), "a retried stage must not overwrite its first attempt");
    assertEquals(0, stages.get(0).getAttemptId());
    assertEquals(1, stages.get(1).getAttemptId());
    assertEquals("FetchFailed", stages.get(0).getFailureReason());
    assertEquals(HudiPhase.DEDUP_AND_INDEX_TAGGING, stages.get(1).getPhase(), "both attempts join to the same phase");
  }

  // -------------------------------------------------------------------------------------------
  // Tolerance to imperfect logs
  // -------------------------------------------------------------------------------------------

  @Test
  void givenTruncatedFinalLine_whenParsing_thenWarnsAndKeepsEarlierEvents() throws IOException {
    // given a log whose last line was cut off mid-write, as happens when an application is killed
    List<String> events = new ArrayList<>();
    events.add(applicationStart("killed-app", "app-2", 1000L));
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 1200L, null));
    Path log = tempDir.resolve("truncated");
    try (BufferedWriter writer = Files.newBufferedWriter(log, StandardCharsets.UTF_8)) {
      for (String event : events) {
        writer.write(event);
        writer.write('\n');
      }
      writer.write("{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":10,\"Task Inf");
    }

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals("killed-app", analysis.getAppName(), "everything before the truncation is still reported");
    assertEquals(1, analysis.getStages().size());
    assertEquals(1, analysis.getWarnings().size(), "the truncated line must be warned about, not thrown on");
    assertTrue(analysis.getWarnings().get(0).contains("could not be parsed"),
        "the warning should say what went wrong: " + analysis.getWarnings().get(0));
  }

  @Test
  void givenUnknownEventTypes_whenParsing_thenIgnoresThemSilently() throws IOException {
    // given a log carrying events from a newer Spark that this tool does not model
    List<String> events = new ArrayList<>();
    events.add("{\"Event\":\"SparkListenerSomeFutureEvent\",\"Shape\":{\"Totally\":\"different\"}}");
    events.add("{\"Event\":\"SparkListenerLogStart\",\"Spark Version\":\"4.0.0\"}");
    events.add(applicationStart("forward-compat", "app-3", 1000L));
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10));
    events.add(stageSubmitted(10, 0, "tag", 1, 1000L));
    events.add(stageCompleted(10, 0, "tag", 1, 1000L, 1200L, null));
    Path log = writeLog("unknown-events", events);

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals("forward-compat", analysis.getAppName());
    assertEquals(1, analysis.getStages().size());
    assertTrue(analysis.getWarnings().isEmpty(), "an unmodelled event is not a problem worth warning about");
  }

  @Test
  void givenGzipCompressedLog_whenParsing_thenReadsItTransparently() throws IOException {
    // given the same events, gzip-compressed
    List<String> events = new ArrayList<>();
    events.add(applicationStart("compressed-app", "app-4", 1000L));
    events.add(jobStart(0, 1000L, "HoodieCompactor:Compacting file slices: t", null, 10));
    events.add(stageSubmitted(10, 0, "compact", 1, 1000L));
    events.add(stageCompleted(10, 0, "compact", 1, 1000L, 1500L, null));

    Path log = tempDir.resolve("compressed.gz");
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        new GZIPOutputStream(Files.newOutputStream(log)), StandardCharsets.UTF_8))) {
      for (String event : events) {
        writer.write(event);
        writer.write('\n');
      }
    }

    // when
    EventLogAnalysis analysis = parse(log);

    // then
    assertEquals("compressed-app", analysis.getAppName());
    assertEquals(HudiPhase.EXECUTION, analysis.getStages().get(0).getPhase());
  }

  @Test
  void givenRollingLogDirectory_whenParsing_thenReadsPartsInNumericOrder() throws IOException {
    // given a rolling log whose parts must be read as 1, 2, 10 rather than 1, 10, 2
    Path logDir = tempDir.resolve("rolling");
    Files.createDirectories(logDir);
    Files.write(logDir.resolve("appstatus_spark-abc"), new byte[0]);
    writeLines(logDir.resolve("events_1_spark-abc"),
        java.util.Collections.singletonList(applicationStart("rolling-app", "app-5", 1000L)));
    writeLines(logDir.resolve("events_2_spark-abc"), java.util.Arrays.asList(
        jobStart(0, 1000L, "HoodieWriteHelper:Tagging: t", null, 10),
        stageSubmitted(10, 0, "tag", 1, 1000L)));
    writeLines(logDir.resolve("events_10_spark-abc"), java.util.Arrays.asList(
        stageCompleted(10, 0, "tag", 1, 1000L, 1200L, null),
        applicationEnd(2000L)));

    // when
    EventLogAnalysis analysis = parse(logDir);

    // then
    assertEquals("rolling-app", analysis.getAppName());
    assertEquals(1000L, analysis.getDurationMillis(), "the end event in the last part must be seen");
    assertEquals(1, analysis.getStages().size());
    assertEquals(200L, analysis.getStages().get(0).getDurationMillis(),
        "submission from part 2 and completion from part 10 must join to one stage");
  }

  @Test
  void givenEmptyFile_whenParsing_thenThrowsRatherThanReportingAnEmptyRun() throws IOException {
    // given
    Path log = tempDir.resolve("empty");
    Files.write(log, new byte[0]);

    // when / then
    IOException thrown = assertThrows(IOException.class, () -> parse(log));
    assertTrue(thrown.getMessage().contains("No parseable Spark events"), thrown.getMessage());
  }

  @Test
  void givenGarbageFile_whenParsing_thenThrowsRatherThanReportingAnEmptyRun() throws IOException {
    // given a file that is not an event log at all
    Path log = tempDir.resolve("garbage");
    Files.write(log, "this is not json\nneither is this\n".getBytes(StandardCharsets.UTF_8));

    // when / then
    IOException thrown = assertThrows(IOException.class, () -> parse(log));
    assertTrue(thrown.getMessage().contains("No parseable Spark events"), thrown.getMessage());
  }

  @Test
  void givenMissingPath_whenParsing_thenThrows() {
    IOException thrown = assertThrows(IOException.class, () -> parse(tempDir.resolve("does-not-exist")));
    assertTrue(thrown.getMessage().contains("does not exist"), thrown.getMessage());
  }

  // -------------------------------------------------------------------------------------------
  // Output
  // -------------------------------------------------------------------------------------------

  @Test
  void givenParsedLog_whenRenderingJson_thenCarriesEveryDocumentedField() throws IOException {
    // given
    Path log = writeLog("json-output", representativeLog());
    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.output = "JSON";

    // when
    ObjectNode root = new HoodieSparkEventLogAnalyzer(cfg, storage()).toJson(parse(log));
    JsonNode reparsed = MAPPER.readTree(MAPPER.writeValueAsString(root));

    // then the envelope
    assertEquals(HoodieSparkEventLogAnalyzer.SCHEMA_VERSION, reparsed.get("schemaVersion").asText());
    assertEquals(log.toString(), reparsed.get("eventLogPath").asText());
    assertTrue(reparsed.get("warnings").isArray());
    assertTrue(reparsed.get("caveats").isArray(), "the envelope must carry the attribution caveats");
    assertTrue(reparsed.get("caveats").size() > 0, "a log with recognised phases always has caveats");

    // then the application block
    JsonNode application = reparsed.get("application");
    assertEquals("rep-app", application.get("name").asText());
    assertEquals("app-6", application.get("id").asText());
    for (String field : new String[] {"durationMillis", "criticalPathMillis", "jobCount", "failedJobCount",
        "stageCount", "failedStageCount", "taskCount", "failedTaskCount", "executorRunTimeMillis",
        "shuffleReadBytes", "shuffleWriteBytes", "memoryBytesSpilled", "diskBytesSpilled",
        "peakConcurrentExecutors", "eventsRead"}) {
      assertTrue(application.get("observed").has(field), "application.observed should carry " + field);
    }

    // then the phase and operation summaries, which share a shape
    JsonNode phases = reparsed.get("phaseSummary");
    assertTrue(phases.isArray() && phases.size() > 0);
    for (String field : new String[] {"phase", "stageCount", "taskCount", "stageWallClockMillis",
        "executorRunTimeMillis", "shuffleBytes", "spilledBytes", "shareOfStageWallClock"}) {
      assertTrue(phases.get(0).has(field), "phaseSummary rows should carry " + field);
    }

    JsonNode operations = reparsed.get("operationSummary");
    assertTrue(operations.isArray() && operations.size() > 0);
    for (String field : new String[] {"operation", "stageCount", "taskCount", "stageWallClockMillis",
        "executorRunTimeMillis", "shuffleBytes", "spilledBytes", "shareOfStageWallClock"}) {
      assertTrue(operations.get(0).has(field), "operationSummary rows should carry " + field);
    }

    // then the stage rows
    JsonNode stages = reparsed.get("stages");
    assertTrue(stages.isArray() && stages.size() > 0);
    JsonNode stage = stages.get(0);
    for (String field : new String[] {"stageId", "attemptId", "name", "jobId", "jobDescription", "module",
        "operation", "operationSource", "phase", "subPhase", "submissionTimeEpochMillis",
        "completionTimeEpochMillis", "failureReason", "observed", "taskFailureReasons"}) {
      assertTrue(stage.has(field), "stage rows should carry " + field);
    }
    for (String field : new String[] {"numTasksPlanned", "numTasksObserved", "durationMillis",
        "executorRunTimeMillis", "executorDeserializeTimeMillis", "jvmGcTimeMillis", "gcFractionOfRunTime",
        "resultSizeBytes", "inputBytes", "inputRecords", "outputBytes", "outputRecords", "shuffleReadBytes",
        "shuffleReadRecords", "shuffleFetchWaitTimeMillis", "shuffleWriteBytes", "shuffleWriteRecords",
        "shuffleWriteTimeNanos", "memoryBytesSpilled", "diskBytesSpilled", "tasksSucceeded", "tasksFailed",
        "tasksKilled", "tasksSpeculative", "skewRatio", "taskDurationMillis"}) {
      assertTrue(stage.get("observed").has(field), "stage.observed should carry " + field);
    }
    JsonNode taskDuration = stage.get("observed").get("taskDurationMillis");
    assertTrue(taskDuration.has("p50") && taskDuration.has("p95") && taskDuration.has("max"));
    assertFalse(taskDuration.has("p99"), "extra percentiles are opt-in via --include-tasks");
  }

  @Test
  void givenIncludeTasks_whenRenderingJson_thenAddsExtraPercentiles() throws IOException {
    // given
    Path log = writeLog("json-tasks", representativeLog());
    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.output = "JSON";
    cfg.includeTasks = true;

    // when
    ObjectNode root = new HoodieSparkEventLogAnalyzer(cfg, storage()).toJson(parse(log));

    // then
    JsonNode taskDuration = root.get("stages").get(0).get("observed").get("taskDurationMillis");
    assertTrue(taskDuration.has("p25") && taskDuration.has("p75") && taskDuration.has("p99"));
  }

  @Test
  void givenTwoRunsOfTheSameLog_whenRenderingJson_thenOutputIsIdentical() throws IOException {
    // given
    Path log = writeLog("deterministic", representativeLog());
    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.output = "JSON";

    // when
    String first = MAPPER.writeValueAsString(new HoodieSparkEventLogAnalyzer(cfg, storage()).toJson(parse(log)));
    String second = MAPPER.writeValueAsString(new HoodieSparkEventLogAnalyzer(cfg, storage()).toJson(parse(log)));

    // then
    assertEquals(first, second, "the JSON contract must be byte-identical across runs");
  }

  @Test
  void givenParsedLog_whenRenderingTable_thenNamesPhasesAndEchoesJobDescription() throws IOException {
    // given
    Path log = writeLog("table-output", representativeLog());

    // when
    String table = new HoodieSparkEventLogAnalyzer(configFor(log), storage()).renderTable(parse(log));

    // then
    assertTrue(table.contains("== Application =="), table);
    assertTrue(table.contains("== Hudi phases, by share of stage wall clock =="), table);
    assertTrue(table.contains("== Hudi operations, by share of stage wall clock =="), table);
    assertTrue(table.contains("WRITE_OPERATION"), "the operation dimension must be visible: " + table);
    assertTrue(table.contains("== How far to trust the attribution =="),
        "the attribution caveats must reach a human reader, not only the JSON: " + table);
    assertTrue(table.contains("INDEX_TAGGING"), "the derived phase is the point of the report");
    assertTrue(table.contains("HoodieWriteHelper:Tagging: my_table"),
        "the verbatim job description must be shown so the phase can be checked");
    assertTrue(table.contains("critical path"), table);
  }

  @Test
  void givenMinStageSecondsFilter_whenRenderingTable_thenOmitsTrivialStages() throws IOException {
    // given a log with a 2s stage and a 100ms stage
    List<String> events = new ArrayList<>();
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: my_table", null, 10, 11));
    events.add(stageSubmitted(10, 0, "slow", 1, 1000L));
    events.add(stageCompleted(10, 0, "slow", 1, 1000L, 3000L, null));
    events.add(stageSubmitted(11, 0, "trivial-stage-name", 1, 3000L));
    events.add(stageCompleted(11, 0, "trivial-stage-name", 1, 3000L, 3100L, null));
    Path log = writeLog("min-seconds", events);

    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.minStageSeconds = 1.0;

    // when
    String table = new HoodieSparkEventLogAnalyzer(cfg, storage()).renderTable(parse(log));

    // then
    assertTrue(table.contains("(1 of 2 shown"), table);
    assertFalse(table.contains("trivial-stage-name"), "a sub-threshold stage must not be printed");
  }

  @Test
  void givenTopNSmallerThanStageCount_whenRenderingTable_thenCapsRowsAtTopN() throws IOException {
    // given
    Path log = writeLog("top-n", representativeLog());
    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.topN = 1;

    // when
    String table = new HoodieSparkEventLogAnalyzer(cfg, storage()).renderTable(parse(log));

    // then
    assertTrue(table.contains("(1 of 2 shown"), table);
  }

  @Test
  void givenOutputFile_whenRunning_thenWritesCleanJsonDocumentToThatFile() throws IOException {
    // given a JSON run directed at a file, so no logging on stdout can interleave with the document
    Path log = writeLog("output-file", representativeLog());
    Path reportFile = tempDir.resolve("report.json");
    HoodieSparkEventLogAnalyzer.Config cfg = configFor(log);
    cfg.output = "JSON";
    cfg.outputFile = reportFile.toString();

    // when
    new HoodieSparkEventLogAnalyzer(cfg, storage()).run();

    // then the file holds a parseable document and nothing else
    JsonNode written = MAPPER.readTree(new String(Files.readAllBytes(reportFile), StandardCharsets.UTF_8));
    assertEquals(HoodieSparkEventLogAnalyzer.SCHEMA_VERSION, written.get("schemaVersion").asText());
    assertEquals("rep-app", written.get("application").get("name").asText());
    assertTrue(written.get("stages").size() > 0);
  }

  @Test
  void givenNegativeDurations_whenFormatting_thenRendersADashRatherThanANonsenseNumber() {
    assertEquals("-", HoodieSparkEventLogAnalyzer.formatMillis(-1L));
    assertEquals("-", HoodieSparkEventLogAnalyzer.formatBytes(-1L));
    assertEquals("1.5s", HoodieSparkEventLogAnalyzer.formatMillis(1500L));
    assertEquals("2m05s", HoodieSparkEventLogAnalyzer.formatMillis(125_000L));
    assertEquals("1.0KB", HoodieSparkEventLogAnalyzer.formatBytes(1024L));
  }

  // -------------------------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------------------------

  /** A log with an application, two phases and a skewed stage, used by the output tests. */
  private List<String> representativeLog() {
    List<String> events = new ArrayList<>();
    events.add(applicationStart("rep-app", "app-6", 1000L));
    events.add(executorAdded("1", 1000L));
    events.add(jobStart(0, 1000L, "HoodieWriteHelper:Tagging: my_table", null, 10));
    events.add(stageSubmitted(10, 0, "mapPartitions at HoodieWriteHelper", 3, 1000L));
    events.add(taskEnd(10, 0, 1000L, 1100L, false, false, false, null, 100L, 10L, 1024L, 0L));
    events.add(taskEnd(10, 0, 1000L, 1200L, false, false, false, null, 200L, 10L, 1024L, 0L));
    events.add(taskEnd(10, 0, 1000L, 2000L, true, false, false, "java.lang.OutOfMemoryError", 1000L, 50L, 2048L, 0L));
    events.add(stageCompleted(10, 0, "mapPartitions at HoodieWriteHelper", 3, 1000L, 2000L, null));
    events.add(jobEnd(0, 2000L, "JobSucceeded"));
    events.add(jobStart(1, 2000L, "BaseSparkCommitActionExecutor:Doing partition and writing data: my_table", null, 11));
    events.add(stageSubmitted(11, 0, "write", 1, 2000L));
    events.add(taskEnd(11, 0, 2000L, 2500L, false, false, false, null, 500L, 20L, 0L, 0L));
    events.add(stageCompleted(11, 0, "write", 1, 2000L, 2500L, null));
    events.add(jobEnd(1, 2500L, "JobSucceeded"));
    events.add(applicationEnd(3000L));
    return events;
  }

  /**
   * One rollback job: the planning and execution descriptions a rollback emits. A restore is simply
   * several of these in one application, which is the only thing distinguishing the two in a log.
   */
  private List<String> rollbackJob(int jobId, int stageId, long start, long end) {
    List<String> events = new ArrayList<>();
    events.add(jobStart(jobId, start, "ListingBasedRollbackStrategy:Creating Listing Rollback Plan: t", null, stageId));
    events.add(stageSubmitted(stageId, 0, "rollback", 1, start));
    events.add(stageCompleted(stageId, 0, "rollback", 1, start, end, null));
    events.add(jobEnd(jobId, end, "JobSucceeded"));
    return events;
  }

  private HoodieSparkEventLogAnalyzer.Config configFor(Path log) {
    HoodieSparkEventLogAnalyzer.Config cfg = new HoodieSparkEventLogAnalyzer.Config();
    cfg.eventLogPath = log.toString();
    return cfg;
  }

  private EventLogAnalysis parse(Path log) throws IOException {
    return new EventLogParser(storage()).parse(new StoragePath(log.toString()));
  }

  private HoodieStorage storage() {
    return HoodieStorageUtils.getStorage(tempDir.toString(), HadoopFSUtils.getStorageConf());
  }

  private Path writeLog(String name, List<String> events) throws IOException {
    Path log = tempDir.resolve(name);
    writeLines(log, events);
    return log;
  }

  private static void writeLines(Path path, List<String> lines) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    }
  }

  // -- Event builders. These emit the same field names Spark's JsonProtocol writes. --------------

  private static String applicationStart(String name, String appId, long timestamp) {
    return String.format("{\"Event\":\"SparkListenerApplicationStart\",\"App Name\":\"%s\","
        + "\"App ID\":\"%s\",\"Timestamp\":%d}", name, appId, timestamp);
  }

  private static String applicationEnd(long timestamp) {
    return String.format("{\"Event\":\"SparkListenerApplicationEnd\",\"Timestamp\":%d}", timestamp);
  }

  private static String jobStart(int jobId, long submissionTime, String description, String jobGroup, int... stageIds) {
    StringBuilder stages = new StringBuilder();
    for (int stageId : stageIds) {
      if (stages.length() > 0) {
        stages.append(',');
      }
      stages.append(stageId);
    }
    // A null description omits the property entirely, which is what an untagged Hudi job really
    // looks like -- Spark simply does not write the key.
    List<String> entries = new ArrayList<>();
    if (description != null) {
      entries.add("\"spark.job.description\":" + quoteJson(description));
    }
    if (jobGroup != null) {
      entries.add("\"spark.jobGroup.id\":" + quoteJson(jobGroup));
    }
    String properties = "{" + String.join(",", entries) + "}";
    return String.format("{\"Event\":\"SparkListenerJobStart\",\"Job ID\":%d,\"Submission Time\":%d,"
        + "\"Stage IDs\":[%s],\"Properties\":%s}", jobId, submissionTime, stages, properties);
  }

  private static String jobEnd(int jobId, long completionTime, String result) {
    return String.format("{\"Event\":\"SparkListenerJobEnd\",\"Job ID\":%d,\"Completion Time\":%d,"
        + "\"Job Result\":{\"Result\":\"%s\"}}", jobId, completionTime, result);
  }

  private static String stageSubmitted(int stageId, int attemptId, String name, int numTasks, long submissionTime) {
    return String.format("{\"Event\":\"SparkListenerStageSubmitted\",\"Stage Info\":{\"Stage ID\":%d,"
            + "\"Stage Attempt ID\":%d,\"Stage Name\":\"%s\",\"Number of Tasks\":%d,\"Submission Time\":%d,"
            + "\"Accumulables\":[]}}",
        stageId, attemptId, name, numTasks, submissionTime);
  }

  private static String stageCompleted(int stageId, int attemptId, String name, int numTasks,
                                       long submissionTime, long completionTime, String failureReason) {
    String failure = failureReason == null ? ""
        : ",\"Failure Reason\":" + quoteJson(failureReason);
    return String.format("{\"Event\":\"SparkListenerStageCompleted\",\"Stage Info\":{\"Stage ID\":%d,"
            + "\"Stage Attempt ID\":%d,\"Stage Name\":\"%s\",\"Number of Tasks\":%d,\"Submission Time\":%d,"
            + "\"Completion Time\":%d,\"Accumulables\":[]%s}}",
        stageId, attemptId, name, numTasks, submissionTime, completionTime, failure);
  }

  /**
   * Builds a TaskEnd event. Shuffle write bytes are set to ten times {@code memorySpilled} so that
   * the aggregation test has a second, independently checkable number.
   */
  private static String taskEnd(int stageId, int attemptId, long launchTime, long finishTime,
                                boolean failed, boolean killed, boolean speculative, String failureClass,
                                long runTime, long gcTime, long memorySpilled, long diskSpilled) {
    String endReason = failureClass == null
        ? "{\"Reason\":\"Success\"}"
        : "{\"Reason\":\"ExceptionFailure\",\"Class Name\":\"" + failureClass + "\",\"Description\":\"boom\"}";
    return String.format("{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":%d,\"Stage Attempt ID\":%d,"
            + "\"Task End Reason\":%s,"
            + "\"Task Info\":{\"Launch Time\":%d,\"Finish Time\":%d,\"Executor ID\":\"1\",\"Failed\":%b,"
            + "\"Killed\":%b,\"Speculative\":%b,\"Locality\":\"PROCESS_LOCAL\"},"
            + "\"Task Metrics\":{\"Executor Run Time\":%d,\"Executor Deserialize Time\":5,\"JVM GC Time\":%d,"
            + "\"Result Size\":100,\"Memory Bytes Spilled\":%d,\"Disk Bytes Spilled\":%d,"
            + "\"Shuffle Read Metrics\":{\"Remote Bytes Read\":10,\"Local Bytes Read\":5,"
            + "\"Fetch Wait Time\":2,\"Total Records Read\":7},"
            + "\"Shuffle Write Metrics\":{\"Shuffle Bytes Written\":%d,\"Shuffle Write Time\":3,"
            + "\"Shuffle Records Written\":4},"
            + "\"Input Metrics\":{\"Bytes Read\":20,\"Records Read\":8},"
            + "\"Output Metrics\":{\"Bytes Written\":0,\"Records Written\":0}}}",
        stageId, attemptId, endReason, launchTime, finishTime, failed, killed, speculative,
        runTime, gcTime, memorySpilled, diskSpilled, memorySpilled * 10);
  }

  private static String executorAdded(String executorId, long timestamp) {
    return String.format("{\"Event\":\"SparkListenerExecutorAdded\",\"Executor ID\":\"%s\",\"Timestamp\":%d}",
        executorId, timestamp);
  }

  private static String executorRemoved(String executorId, long timestamp, String reason) {
    return String.format("{\"Event\":\"SparkListenerExecutorRemoved\",\"Executor ID\":\"%s\","
        + "\"Timestamp\":%d,\"Removed Reason\":%s}", executorId, timestamp, quoteJson(reason));
  }

  private static String quoteJson(String value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Guards against an accidental hard dependency on Spark classes in this test class. */
  @Test
  void givenThisTestClass_whenInspectingItsFixtures_thenNoSparkTypeIsInvolved() {
    List<String> sparkFields = new ArrayList<>();
    for (java.lang.reflect.Field field : getClass().getDeclaredFields()) {
      if (field.getType().getName().startsWith("org.apache.spark")) {
        sparkFields.add(field.getName());
      }
    }
    assertTrue(sparkFields.isEmpty(), "the analyzer must stay runnable without Spark, but found " + sparkFields);
    assertNotNull(tempDir);
  }
}
