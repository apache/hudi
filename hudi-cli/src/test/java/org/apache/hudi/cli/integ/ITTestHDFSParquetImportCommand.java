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

package org.apache.hudi.cli.integ;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.HDFSParquetImporter;
import org.apache.hudi.utilities.functional.TestHDFSParquetImporter;
import org.apache.hudi.utilities.functional.TestHDFSParquetImporter.HoodieTripModel;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.HDFSParquetImportCommand}.
 */
@Disabled("Disable due to flakiness and feature deprecation.")
public class ITTestHDFSParquetImportCommand extends HoodieCLIIntegrationTestBase {

  private Path sourcePath;
  private Path targetPath;
  private String tableName;
  private String schemaFile;
  private String tablePath;

  private List<GenericRecord> insertData;
  private TestHDFSParquetImporter importer;

  @BeforeEach
  public void init() throws IOException, ParseException {
    tableName = "test_table";
    tablePath = basePath + Path.SEPARATOR + tableName;
    sourcePath = new Path(basePath, "source");
    targetPath = new Path(tablePath);
    schemaFile = new Path(basePath, "file.schema").toString();

    // create schema file
    try (FSDataOutputStream schemaFileOS = fs.create(new Path(schemaFile))) {
      schemaFileOS.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA.getBytes());
    }

    importer = new TestHDFSParquetImporter();
    insertData = importer.createInsertRecords(sourcePath);
  }

  /**
   * Test case for 'hdfsparquetimport' with insert.
   */
  @Test
  public void testConvertWithInsert() throws IOException {
    String command = String.format("hdfsparquetimport --srcPath %s --targetPath %s --tableName %s "
        + "--tableType %s --rowKeyField %s" + " --partitionPathField %s --parallelism %s "
        + "--schemaFilePath %s --format %s --sparkMemory %s --retry %s --sparkMaster %s",
        sourcePath.toString(), targetPath.toString(), tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "_row_key", "timestamp", "1", schemaFile, "parquet", "2G", "1", "local");
    CommandResult cr = getShell().executeCommand(command);

    assertAll("Command run success",
        () -> assertTrue(cr.isSuccess()),
        () -> assertEquals("Table imported to hoodie format", cr.getResult().toString()));

    // Check hudi table exist
    String metaPath = targetPath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    assertTrue(Files.exists(Paths.get(metaPath)), "Hoodie table not exist.");

    // Load meta data
    new TableCommand().connect(targetPath.toString(), TimelineLayoutVersion.VERSION_1, false, 2000, 300000, 7);
    metaClient = HoodieCLI.getTableMetaClient();

    assertEquals(1, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "Should only 1 commit.");

    verifyResultData(insertData);
  }

  /**
   * Test case for 'hdfsparquetimport' with upsert.
   */
  @Test
  public void testConvertWithUpsert() throws IOException, ParseException {
    Path upsertFolder = new Path(basePath, "testUpsertSrc");
    List<GenericRecord> upsertData = importer.createUpsertRecords(upsertFolder);

    // first insert records
    HDFSParquetImporter.Config cfg = importer.getHDFSParquetImporterConfig(sourcePath.toString(), tablePath,
        tableName, HoodieTableType.COPY_ON_WRITE.name(), "_row_key", "timestamp", 1, schemaFile);
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);

    dataImporter.dataImport(jsc, 0);

    // Load meta data
    new TableCommand().connect(targetPath.toString(), TimelineLayoutVersion.VERSION_1, false, 2000, 300000, 7);
    metaClient = HoodieCLI.getTableMetaClient();

    // check if insert instant exist
    assertEquals(1, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "Should only 1 commit.");

    String command = String.format("hdfsparquetimport --srcPath %s --targetPath %s --tableName %s "
        + "--tableType %s --rowKeyField %s" + " --partitionPathField %s --parallelism %s "
        + "--schemaFilePath %s --format %s --sparkMemory %s --retry %s --sparkMaster %s --upsert %s",
        upsertFolder.toString(), targetPath.toString(), tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "_row_key", "timestamp", "1", schemaFile, "parquet", "2G", "1", "local", "true");
    CommandResult cr = getShell().executeCommand(command);

    assertAll("Command run success",
        () -> assertTrue(cr.isSuccess()),
        () -> assertEquals("Table imported to hoodie format", cr.getResult().toString()));

    // reload meta client
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "Should have 2 commit.");

    // construct result, remove top 10 and add upsert data.
    List<GenericRecord> expectData = insertData.subList(11, 96);
    expectData.addAll(upsertData);

    verifyResultData(expectData);
  }

  /**
   * Method to verify result is equals to expect.
   */
  private void verifyResultData(List<GenericRecord> expectData) {
    Dataset<Row> ds = HoodieClientTestUtils.read(jsc, tablePath, sqlContext, fs, tablePath + "/*/*/*/*");

    List<Row> readData = ds.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon").collectAsList();
    List<HoodieTripModel> result = readData.stream().map(row ->
        new HoodieTripModel(row.getLong(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(4),
            row.getDouble(5), row.getDouble(6), row.getDouble(7)))
        .collect(Collectors.toList());

    List<HoodieTripModel> expected = expectData.stream().map(g ->
        new HoodieTripModel(Long.parseLong(g.get("timestamp").toString()),
            g.get("_row_key").toString(),
            g.get("rider").toString(),
            g.get("driver").toString(),
            Double.parseDouble(g.get("begin_lat").toString()),
            Double.parseDouble(g.get("begin_lon").toString()),
            Double.parseDouble(g.get("end_lat").toString()),
            Double.parseDouble(g.get("end_lon").toString())))
        .collect(Collectors.toList());

    assertAll("Result list equals",
        () -> assertEquals(expected.size(), result.size()),
        () -> assertTrue(result.containsAll(expected) && expected.containsAll(result)));
  }
}
