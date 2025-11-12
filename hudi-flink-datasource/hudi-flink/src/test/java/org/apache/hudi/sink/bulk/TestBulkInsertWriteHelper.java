/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk;

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link BulkInsertWriterHelper}.
 */
public class TestBulkInsertWriteHelper {
  protected Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testWrite() throws Exception {
    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    String instant = WriteClientTestUtils.createNewInstantTime();
    RowType rowType = TestConfigurations.ROW_TYPE;
    BulkInsertWriterHelper writerHelper = new BulkInsertWriterHelper(conf, table, table.getConfig(), instant,
        1, 1, 0, rowType, false);
    for (RowData row: TestData.DATA_SET_INSERT) {
      writerHelper.write(row);
    }
    List<WriteStatus> writeStatusList = writerHelper.getWriteStatuses(1);
    assertWriteStatus(writeStatusList);

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1, id2,par1,id2,Stephen,33,2,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3,par2, id4,par2,id4,Fabian,31,4,par2]");
    expected.put("par3", "[id5,par3,id5,Sophia,18,5,par3, id6,par3,id6,Emma,20,6,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");

    TestData.checkWrittenData(tempFile, expected);

    // set up preserveHoodieMetadata as true and check again
    RowType rowType2 = DataTypeUtils.addMetadataFields(rowType, false);
    BulkInsertWriterHelper writerHelper2 = new BulkInsertWriterHelper(conf, table, table.getConfig(), instant,
        1, 1, 0, rowType2, true);
    for (RowData row: rowsWithMetadata(instant, TestData.DATA_SET_INSERT)) {
      writerHelper.write(row);
    }
    List<WriteStatus> writeStatusList2 = writerHelper.getWriteStatuses(1);
    assertWriteStatus(writeStatusList2);

    String expectRows = "[" + instant + ", " + instant + "]";
    Map<String, String> expected2 = new HashMap<>();
    expected2.put("par1", expectRows);
    expected2.put("par2", expectRows);
    expected2.put("par3", expectRows);
    expected2.put("par4", expectRows);

    TestData.checkWrittenData(tempFile, expected2, 4, TestBulkInsertWriteHelper::filterCommitTime);
  }

  private void assertWriteStatus(List<WriteStatus> writeStatusList) {
    String partitions = writeStatusList.stream()
        .map(writeStatus -> StringUtils.nullToEmpty(writeStatus.getStat().getPartitionPath()))
        .sorted()
        .collect(Collectors.joining(","));
    assertThat(partitions, is("par1,par2,par3,par4"));
    List<String> files = writeStatusList.stream()
        .map(writeStatus -> writeStatus.getStat().getPath())
        .collect(Collectors.toList());
    assertThat(files.size(), is(4));
  }

  private static List<RowData> rowsWithMetadata(String instantTime, List<RowData> rows) {
    List<RowData> rowsWithMetadata = new ArrayList<>();
    int seqNum = 0;
    for (RowData row : rows) {
      GenericRowData rebuilt = new GenericRowData(row.getArity() + 5);
      rebuilt.setField(0, StringData.fromString(instantTime));
      rebuilt.setField(1, seqNum++);
      rebuilt.setField(2, row.getString(0));
      rebuilt.setField(3, row.getString(4));
      rebuilt.setField(4, StringData.fromString("f" + seqNum));
    }
    return rowsWithMetadata;
  }

  private static String filterCommitTime(GenericRecord genericRecord) {
    return genericRecord.get("_hoodie_commit_time").toString();
  }
}
