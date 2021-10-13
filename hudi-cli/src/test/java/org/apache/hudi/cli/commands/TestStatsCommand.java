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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class of {@link org.apache.hudi.cli.commands.StatsCommand}.
 */
@Tag("functional")
public class TestStatsCommand extends CLIFunctionalTestHarness {

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = tableName();
    tablePath = tablePath(tableName);

    HoodieCLI.conf = hadoopConf();
    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case for command 'stats wa'.
   */
  @Test
  public void testWriteAmplificationStats() throws Exception {
    // generate data and metadata
    Map<String, Integer[]> data = new LinkedHashMap<>();
    data.put("100", new Integer[] {15, 10});
    data.put("101", new Integer[] {20, 10});
    data.put("102", new Integer[] {15, 15});

    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String k = entry.getKey();
      Integer[] v = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, k, hadoopConf(),
          Option.of(v[0]), Option.of(v[1]));
    }

    CommandResult cr = shell().executeCommand("stats wa");
    assertTrue(cr.isSuccess());

    // generate expect
    List<Comparable[]> rows = new ArrayList<>();
    DecimalFormat df = new DecimalFormat("#.00");
    data.forEach((key, value) -> {
      // there are two partitions, so need to *2
      rows.add(new Comparable[] {key, value[1] * 2, value[0] * 2, df.format((float) value[0] / value[1])});
    });
    int totalWrite = data.values().stream().map(integers -> integers[0] * 2).mapToInt(s -> s).sum();
    int totalUpdate = data.values().stream().map(integers -> integers[1] * 2).mapToInt(s -> s).sum();
    rows.add(new Comparable[] {"Total", totalUpdate, totalWrite, df.format((float) totalWrite / totalUpdate)});

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPSERTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_WRITE_AMPLIFICATION_FACTOR);
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for command 'stats filesizes'.
   */
  @Test
  public void testFileSizeStats() throws Exception {
    String commit1 = "100";
    String commit2 = "101";
    Map<String, Integer[]> data = new LinkedHashMap<>();
    data.put(commit1, new Integer[] {100, 120, 150});
    data.put(commit2, new Integer[] {200, 180, 250, 300});

    // generate data file
    String partition1 = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String partition2 = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    String partition3 = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;

    HoodieTestTable testTable = HoodieTestTable.of(HoodieCLI.getTableMetaClient());
    Integer[] data1 = data.get(commit1);
    assertTrue(3 <= data1.length);
    testTable.addCommit(commit1)
        .withBaseFilesInPartition(partition1, data1[0])
        .withBaseFilesInPartition(partition2, data1[1])
        .withBaseFilesInPartition(partition3, data1[2]);

    Integer[] data2 = data.get(commit2);
    assertTrue(4 <= data2.length);
    testTable.addCommit(commit2)
        .withBaseFilesInPartition(partition1, data2[0])
        .withBaseFilesInPartition(partition2, data2[1], data2[2])
        .withBaseFilesInPartition(partition3, data2[3]);

    CommandResult cr = shell().executeCommand("stats filesizes");
    assertTrue(cr.isSuccess());

    Histogram globalHistogram = new Histogram(new UniformReservoir(StatsCommand.MAX_FILES));
    HashMap<String, Histogram> commitHistoMap = new HashMap<>();
    data.forEach((k, v) -> {
      commitHistoMap.put(k, new Histogram(new UniformReservoir(StatsCommand.MAX_FILES)));
      for (int value : v) {
        commitHistoMap.get(k).update(value);
        globalHistogram.update(value);
      }
    });

    // generate expect
    List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, Histogram> entry : commitHistoMap.entrySet()) {
      Snapshot s = entry.getValue().getSnapshot();
      rows.add(new StatsCommand().printFileSizeHistogram(entry.getKey(), s));
    }
    Snapshot s = globalHistogram.getSnapshot();
    rows.add(new StatsCommand().printFileSizeHistogram("ALL", s));

    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_MIN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_10TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_50TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_AVG)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_95TH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_MAX)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_NUM_FILES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HISTOGRAM_STD_DEV);
    String expect = HoodiePrintHelper.print(header, new StatsCommand().getFieldNameToConverterMap(),
        "", false, -1, false, rows);
    expect = removeNonWordAndStripSpace(expect);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expect, got);
  }
}
