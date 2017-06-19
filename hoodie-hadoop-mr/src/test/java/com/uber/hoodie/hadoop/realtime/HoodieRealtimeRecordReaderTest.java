/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;


import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.hadoop.InputFormatTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class HoodieRealtimeRecordReaderTest {

    private JobConf jobConf;

    @Before
    public void setUp() {
        jobConf = new JobConf();
    }

    @Rule
    public TemporaryFolder basePath = new TemporaryFolder();

    private HoodieLogFormat.Writer writeLogFile(File partitionDir, Schema schema, String fileId,
                                                String baseCommit, String newCommit, int numberOfRecords) throws InterruptedException,IOException {
        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(partitionDir.getPath()))
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId)
                .overBaseCommit(baseCommit).withFs(FSUtils.getFs()).build();
        List<IndexedRecord> records = new ArrayList<>();
        for(int i=0; i < numberOfRecords; i++) {
            records.add(InputFormatTestUtil.generateAvroRecord(schema, i, newCommit, "fileid0"));
        }
        Schema writeSchema = records.get(0).getSchema();
        HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, writeSchema);
        writer = writer.appendBlock(dataBlock);
        long size = writer.getCurrentSize();
        return writer;
    }

    @Test
    public void testReader() throws Exception {
        // initial commit
        Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
        HoodieTestUtils.initTableType(basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
        String commitTime = "100";
        File partitionDir = InputFormatTestUtil.prepareParquetDataset(basePath, schema, 1, 100, commitTime);
        InputFormatTestUtil.commit(basePath, commitTime);
        // Add the paths
        FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

        // update files or generate new log file
        String newCommitTime = "101";
        HoodieLogFormat.Writer writer = writeLogFile(partitionDir, schema, "fileid0", commitTime, newCommitTime, 100);
        long size = writer.getCurrentSize();
        writer.close();
        assertTrue("block - size should be > 0", size > 0);

        //create a split with baseFile (parquet file written earlier) and new log file(s)
        String logFilePath = writer.getLogFile().getPath().toString();
        HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(new FileSplit(new Path(partitionDir
                + "/fileid0_1_" + commitTime + ".parquet"),0,1,jobConf), Arrays.asList(logFilePath), newCommitTime);

        //create a RecordReader to be used by HoodieRealtimeRecordReader
        RecordReader<Void, ArrayWritable> reader =
                new MapredParquetInputFormat().
                        getRecordReader(new FileSplit(split.getPath(), 0,
                                        FSUtils.getFs().getLength(split.getPath()), (String[]) null), jobConf, null);
        JobConf jobConf = new JobConf();
        List<Schema.Field> fields = schema.getFields();
        String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
        String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
        jobConf.set("partition_columns", "datestr");

        //validate record reader compaction
        HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

        //use reader to read base Parquet File and log file, merge in flight and return latest commit
        //here all 100 records should be updated, see above
        Void key = recordReader.createKey();
        ArrayWritable value = recordReader.createValue();
        while(recordReader.next(key, value)) {
            Writable[] values = value.get();
            //check if the record written is with latest commit, here "101"
            Assert.assertEquals(values[0].toString(), newCommitTime);
            key = recordReader.createKey();
            value = recordReader.createValue();
        }
    }

}
