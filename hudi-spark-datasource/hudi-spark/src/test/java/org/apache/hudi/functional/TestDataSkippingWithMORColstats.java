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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordToString;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS;
import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test mor with colstats enabled in scenarios to ensure that files
 * are being appropriately read or not read.
 * The strategy employed is to corrupt targeted base files. If we want
 * to prove the file is read, we assert that an exception will be thrown.
 * If we want to prove the file is not read, we expect the read to
 * successfully execute.
 */
public class TestDataSkippingWithMORColstats extends HoodieSparkClientTestBase {

  private static String matchCond = "trip_type = 'UBERX'";
  private static String nonMatchCond = "trip_type = 'BLACK'";
  private static String[] dropColumns = {"_hoodie_commit_time", "_hoodie_commit_seqno",
      "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name"};

  private Boolean shouldOverwrite;
  Map<String, String> options;
  @TempDir
  public java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    dataGen = new HoodieTestDataGenerator();
    shouldOverwrite = true;
    options = getOptions();
    Properties props = new Properties();
    props.putAll(options);
    try {
      metaClient = HoodieTableMetaClient.newTableBuilder()
          .fromProperties(props)
          .setTableType(HoodieTableType.MERGE_ON_READ.name())
          .initTable(storageConf.newInstance(), basePath.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    metaClient = null;
  }

  /**
   * Create two files, one should be excluded by colstats
   */
  @Test
  public void testBaseFileOnly() {
    // note that this config is here just to test that it does nothing since
    // we have specified which columns to index
    options.put(HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.key(), "1");

    Dataset<Row> inserts = makeInsertDf("000", 100);
    Dataset<Row> batch1 = inserts.where(matchCond);
    Dataset<Row> batch2 = inserts.where(nonMatchCond);
    doWrite(batch1);
    doWrite(batch2);
    List<Path> filesToCorrupt = getFilesToCorrupt();
    assertEquals(1, filesToCorrupt.size());
    filesToCorrupt.forEach(TestDataSkippingWithMORColstats::corruptFile);
    assertEquals(0, readMatchingRecords().except(batch1).count());
    //Read without data skipping to show that it will fail
    //Reading with data skipping succeeded so that means that data skipping is working and the corrupted
    //file was not read
    assertThrows(SparkException.class, () -> readMatchingRecords(false).count());
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file so that both file groups match
   * both file groups must be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatches() {
    testBaseFileAndLogFileUpdateMatchesHelper(false, false,false, false);
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file so that both file groups match
   * Then do a compaction
   * Now you have two base files that match
   * both file groups must be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatchesDoCompaction() {
    testBaseFileAndLogFileUpdateMatchesHelper(false, true,false, false);
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file for each filegroup that contains exactly the same records as the base file
   * Then schedule an async compaction
   * Then add a log file so that both file groups match the condition
   * The new log file is a member of a newer file slice
   * both file groups must be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatchesScheduleCompaction() {
    testBaseFileAndLogFileUpdateMatchesHelper(true, false,false, false);
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file so that both file groups match the condition
   * Then add a delete for that record so that the file group no longer matches the condition
   * both file groups must still be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatchesDeleteBlock() {
    testBaseFileAndLogFileUpdateMatchesHelper(false, false,true, false);
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file so that both file groups match the condition
   * Then add a delete for that record so that the file group no longer matches the condition
   * Then compact
   * Only the first file group needs to be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatchesDeleteBlockCompact() {
    testBaseFileAndLogFileUpdateMatchesHelper(false, true,true, false);
  }

  /**
   * Create two base files, One base file doesn't match the condition
   * Then add a log file so that both file groups match the condition
   * Then delete the deltacommit and write the original value for the
   *    record so that a rollback is triggered and the file group no
   *    longer matches the condition
   * both filegroups should be read
   */
  @Test
  public void testBaseFileAndLogFileUpdateMatchesAndRollBack() {
    testBaseFileAndLogFileUpdateMatchesHelper(false, false,false, true);
  }

  /**
   * Test where one filegroup doesn't match the condition, then update so both filegroups match
   */
  private void testBaseFileAndLogFileUpdateMatchesHelper(Boolean shouldScheduleCompaction,
                                                         Boolean shouldInlineCompact,
                                                         Boolean shouldDelete,
                                                         Boolean shouldRollback) {
    Dataset<Row> inserts = makeInsertDf("000", 100);
    Dataset<Row> batch1 = inserts.where(matchCond);
    Dataset<Row> batch2 = inserts.where(nonMatchCond);
    doWrite(batch1);
    doWrite(batch2);
    if (shouldScheduleCompaction) {
      doWrite(inserts);
      scheduleCompaction();
    }
    List<Path> filesToCorrupt = getFilesToCorrupt();
    assertEquals(1, filesToCorrupt.size());
    Dataset<Row> recordToUpdate = batch2.limit(1);
    Dataset<Row> updatedRecord = makeRecordMatch(recordToUpdate);
    doWrite(updatedRecord);
    if (shouldRollback) {
      deleteLatestDeltacommit();
      enableInlineCompaction(shouldInlineCompact);
      doWrite(recordToUpdate);
      assertEquals(0, readMatchingRecords().except(batch1).count());
    } else if (shouldDelete) {
      enableInlineCompaction(shouldInlineCompact);
      doDelete(updatedRecord);
      assertEquals(0, readMatchingRecords().except(batch1).count());
    } else {
      assertEquals(0, readMatchingRecords().except(batch1.union(updatedRecord)).count());
    }

    if (shouldInlineCompact) {
      filesToCorrupt = getFilesToCorrupt();
      filesToCorrupt.forEach(TestDataSkippingWithMORColstats::corruptFile);
      if (shouldDelete || shouldRollback) {
        assertEquals(1, filesToCorrupt.size());
        assertEquals(0, readMatchingRecords().except(batch1).count());
      } else {
        enableInlineCompaction(true);
        doWrite(updatedRecord);
        assertEquals(0, filesToCorrupt.size());
      }
    } else {
      //Corrupt to prove that colstats does not exclude filegroup
      filesToCorrupt.forEach(TestDataSkippingWithMORColstats::corruptFile);
      assertEquals(1, filesToCorrupt.size());
      if (shouldRollback) {
        // the update log files got deleted during rollback,
        // so the cols tats does not include the file groups with corrupt files.
        assertDoesNotThrow(() -> readMatchingRecords().count(), "The non match file group got rollback correctly");
      } else {
        assertThrows(SparkException.class, () -> readMatchingRecords().count(), "The corrupt parquets are not excluded");
      }
    }
  }

  /**
   * Create two base files, One base file all records match the condition.
   * The other base file has one record that matches the condition.
   * Then add a log file that makes that one matching record not match anymore.
   * both file groups must be read even though no records from the second file slice
   * will pass the condition after mor merging
   */
  @Test
  public void testBaseFileAndLogFileUpdateUnmatches() {
    testBaseFileAndLogFileUpdateUnmatchesHelper(false);
  }

  /**
   * Create two base files, One base file all records match the condition.
   * The other base file has one record that matches the condition.
   * Then add a log file for each filegroup that contains exactly the same records as the base file
   * Then schedule a compaction
   * Then add a log file that makes that one matching record not match anymore.
   * The new log file is a member of a newer file slice
   * both file groups must be read even though no records from the second file slice
   * will pass the condition after mor merging
   */
  @Test
  public void testBaseFileAndLogFileUpdateUnmatchesScheduleCompaction() {
    testBaseFileAndLogFileUpdateUnmatchesHelper(true);
  }

  /**
   * Test where one filegroup all records match the condition and the other has only a single record that matches
   * an update is added that makes the second filegroup no longer match
   * Dataskipping should not exclude the second filegroup
   */
  private void testBaseFileAndLogFileUpdateUnmatchesHelper(Boolean shouldScheduleCompaction) {
    Dataset<Row> inserts = makeInsertDf("000", 100);
    Dataset<Row> batch1 = inserts.where(matchCond);
    doWrite(batch1);
    //no matches in batch2
    Dataset<Row> batch2 = inserts.where(nonMatchCond);
    //make 1 record match
    Dataset<Row> recordToMod = batch2.limit(1);
    Dataset<Row> initialRecordToMod = makeRecordMatch(recordToMod);
    Dataset<Row> modBatch2 = removeRecord(batch2, recordToMod).union(initialRecordToMod);
    doWrite(modBatch2);
    if (shouldScheduleCompaction) {
      doWrite(batch1.union(modBatch2));
      scheduleCompaction();
    }

    //update batch2 so no matching records in the filegroup
    doWrite(recordToMod);
    assertEquals(0, readMatchingRecords().except(batch1).count());

    //Corrupt to prove that colstats does not exclude filegroup
    List<Path> filesToCorrupt = getFilesToCorrupt();
    assertEquals(1, filesToCorrupt.size());
    filesToCorrupt.forEach(TestDataSkippingWithMORColstats::corruptFile);
    assertThrows(SparkException.class, () -> readMatchingRecords().count());
  }

  private Map<String, String> getOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    options.put(HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key(), "trip_type");
    options.put(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true");
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    options.put(HoodieWriteConfig.TBL_NAME.key(), "testTable");
    options.put(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    options.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    options.put(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0");
    options.put(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), "false");
    options.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
    return options;
  }

  private void scheduleCompaction() {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath.toString())
        .withRollbackUsingMarkers(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withColumnStatsIndexForColumns("trip_type").build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .forTable("testTable")
        .withKeyGenerator("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      client.scheduleCompaction(Option.empty());
    }
  }

  /**
   * remove recordToRemove from batch
   * recordToRemove is expected to only have 1 row
   */
  private Dataset<Row> removeRecord(Dataset<Row> batch, Dataset<Row> recordToRemove) {
    return batch.where("_row_key != '" + recordToRemove.first().getString(1) + "'");
  }

  /**
   * Returns a list of the base parquet files for the latest fileslice in it's filegroup where
   * no records match the condition
   */
  private List<Path> getFilesToCorrupt() {
    Set<String> fileIds = new HashSet<>();
    sparkSession.read().format("hudi").load(basePath.toString())
            .where(matchCond)
            .select("_hoodie_file_name").distinct()
        .collectAsList().forEach(row -> {
          String fileName = row.getString(0);
          fileIds.add(FSUtils.getFileIdFromFileName(fileName));
        });

    try (Stream<Path> stream = Files.list(basePath)) {
      Map<String,Path> latestBaseFiles = new HashMap<>();
      List<Path> files = stream
          .filter(file -> !Files.isDirectory(file))
          .filter(file -> file.toString().contains(".parquet"))
          .filter(file -> !file.toString().contains(".crc"))
          .filter(file -> !fileIds.contains(FSUtils.getFileIdFromFileName(file.getFileName().toString())))
          .collect(Collectors.toList());
      files.forEach(f -> {
        String fileID = FSUtils.getFileIdFromFileName(f.getFileName().toString());
        if (!latestBaseFiles.containsKey(fileID) || FSUtils.getCommitTime(f.getFileName().toString())
            .compareTo(FSUtils.getCommitTime(latestBaseFiles.get(fileID).getFileName().toString())) > 0) {
          latestBaseFiles.put(fileID, f);
        }
      });
      return new ArrayList<>(latestBaseFiles.values());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void doWrite(Dataset<Row> df) {
    if (shouldOverwrite) {
      shouldOverwrite = false;
      df.write().format("hudi").options(options).mode(Overwrite).save(basePath.toString());
    } else {
      df.write().format("hudi").options(options).mode(Append).save(basePath.toString());
    }
  }

  private void doDelete(Dataset<Row> df) {
    df.write().format("hudi").options(options).option(DataSourceWriteOptions.OPERATION().key(),
        DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL()).mode(Append).save(basePath.toString());
  }

  /**
   * update rowToMod to make it match the condition.
   * rowToMod is expected to only have 1 row
   */
  private Dataset<Row> makeRecordMatch(Dataset<Row> rowToMod) {
    return updateTripType(rowToMod, "UBERX");
  }

  private Dataset<Row> updateTripType(Dataset<Row> rowToMod, String value) {
    rowToMod.createOrReplaceTempView("rowToMod");
    return sparkSession.sqlContext().createDataFrame(sparkSession.sql("select _hoodie_is_deleted, _row_key, "
        + "begin_lat, begin_lon, current_date, current_ts, distance_in_meters, driver, end_lat, end_lon, fare, height, "
        + "nation, partition, partition_path, rider, seconds_since_epoch, timestamp, tip_history, '" + value
        + "' as trip_type, weight from rowToMod").rdd(), rowToMod.schema());
  }

  /**
   * Read records from Hudi that match the condition
   * and drop the meta cols
   */
  private Dataset<Row> readMatchingRecords() {
    return readMatchingRecords(true);
  }

  public Dataset<Row> readMatchingRecords(Boolean useDataSkipping) {
    if (useDataSkipping) {
      return sparkSession.read().format("hudi").options(options)
          .load(basePath.toString()).where(matchCond).drop(dropColumns);
    } else {
      return sparkSession.read().format("hudi")
          .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "false")
          .load(basePath.toString()).where(matchCond).drop(dropColumns);
    }
  }

  /**
   * Corrupt a parquet file by deleting it and replacing
   * it with an empty file
   */
  protected static void corruptFile(Path path) {
    File fileToCorrupt = path.toFile();
    fileToCorrupt.delete();
    try {
      fileToCorrupt.createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    //cant do df.except with city_to_state and our testing is for the
    //col stats index so it is ok to just drop this here
    return sparkSession.read().json(rdd).drop("city_to_state");
  }

  public void deleteLatestDeltacommit() {
    String filename = INSTANT_FILE_NAME_GENERATOR.getFileName(metaClient.getActiveTimeline().lastInstant().get());
    File deltacommit = new File(metaClient.getBasePath() + "/.hoodie/timeline/" + filename);
    deltacommit.delete();
  }

  /**
   * Need to enable inline compaction before final write. We need to do this
   * before the final write instead of setting a num delta commits number
   * because in the case of rollback, we do 3 updates and then rollback
   * and do an update, but we only want to compact the second time
   * we have 3
   */
  public void enableInlineCompaction(Boolean shouldEnable) {
    if (shouldEnable) {
      this.options.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
      this.options.put(INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
    }
  }
}
