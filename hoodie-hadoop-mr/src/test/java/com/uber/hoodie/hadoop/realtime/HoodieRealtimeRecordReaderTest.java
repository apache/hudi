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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
                .overBaseCommit(baseCommit).withFs(FSUtils.getFs(basePath.getRoot().getAbsolutePath())).build();
        List<IndexedRecord> records = new ArrayList<>();
        for(int i=0; i < numberOfRecords; i++) {
            records.add(InputFormatTestUtil.generateAvroRecordFromJson(schema, i, newCommit, "fileid0"));
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
                                        FSUtils.getFs(basePath.getRoot().getAbsolutePath()).getLength(split.getPath()), (String[]) null), jobConf, null);
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

    @Test
    public void testReaderWithNestedAndComplexSchema() throws Exception {
        // initial commit
        Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getComplexEvolvedSchema());
        HoodieTestUtils.initTableType(basePath.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
        String commitTime = "100";
        int numberOfRecords = 100;
        int numberOfLogRecords = numberOfRecords / 2;
        File partitionDir = InputFormatTestUtil.prepareParquetDataset(basePath, schema, 1, numberOfRecords, commitTime);
        InputFormatTestUtil.commit(basePath, commitTime);
        // Add the paths
        FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

        // update files or generate new log file
        String newCommitTime = "101";
        HoodieLogFormat.Writer writer = writeLogFile(partitionDir, schema, "fileid0", commitTime, newCommitTime, numberOfLogRecords);
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
              FSUtils.getFs(basePath.toString()).getLength(split.getPath()), (String[]) null), jobConf, null);
        JobConf jobConf = new JobConf();
        List<Schema.Field> fields = schema.getFields();
        String names = fields.stream().map(f -> f.name()).collect(Collectors.joining(","));
        String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
        jobConf.set("partition_columns", "datestr");

        // validate record reader compaction
        HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

        // use reader to read base Parquet File and log file, merge in flight and return latest commit
        // here the first 50 records should be updated, see above
        Void key = recordReader.createKey();
        ArrayWritable value = recordReader.createValue();
        int numRecordsRead = 0;
        while (recordReader.next(key, value)) {
            int currentRecordNo = numRecordsRead;
            ++numRecordsRead;
            Writable[] values = value.get();
            String recordCommitTime;
            //check if the record written is with latest commit, here "101"
            if (numRecordsRead > numberOfLogRecords) {
                recordCommitTime = commitTime;
            } else {
                recordCommitTime = newCommitTime;
            }
            String recordCommitTimeSuffix = "@" + recordCommitTime;

            Assert.assertEquals(values[0].toString(), recordCommitTime);
            key = recordReader.createKey();
            value = recordReader.createValue();

            // Assert type STRING
            Assert.assertEquals("test value for field: field1", values[5].toString(), "field" + currentRecordNo);
            Assert.assertEquals("test value for field: field2",values[6].toString(), "field" + currentRecordNo + recordCommitTimeSuffix);
            Assert.assertEquals("test value for field: name", values[7].toString(), "name" + currentRecordNo);

            // Assert type INT
            IntWritable intWritable = (IntWritable)values[8];
            Assert.assertEquals("test value for field: favoriteIntNumber", intWritable.get(), currentRecordNo + recordCommitTime.hashCode());

            // Assert type LONG
            LongWritable longWritable = (LongWritable)values[9];
            Assert.assertEquals("test value for field: favoriteNumber", longWritable.get(), currentRecordNo + recordCommitTime.hashCode());

            // Assert type FLOAT
            FloatWritable floatWritable = (FloatWritable)values[10];
            Assert.assertEquals("test value for field: favoriteFloatNumber", floatWritable.get(), (float)((currentRecordNo + recordCommitTime.hashCode()) / 1024.0), 0);

            // Assert type DOUBLE
            DoubleWritable doubleWritable = (DoubleWritable)values[11];
            Assert.assertEquals("test value for field: favoriteDoubleNumber", doubleWritable.get(), (currentRecordNo + recordCommitTime.hashCode()) / 1024.0, 0);

            // Assert type MAP
            ArrayWritable mapItem = (ArrayWritable)values[12];
            Writable[] mapItemValues = mapItem.get();
            ArrayWritable mapItemValue1 = (ArrayWritable)mapItemValues[0];
            ArrayWritable mapItemValue2 = (ArrayWritable)mapItemValues[1];
            Assert.assertEquals("test value for field: tags", mapItemValue1.get()[0].toString(), "mapItem1");
            Assert.assertEquals("test value for field: tags", mapItemValue2.get()[0].toString(), "mapItem2");
            ArrayWritable mapItemValue1value = (ArrayWritable)mapItemValue1.get()[1];
            ArrayWritable mapItemValue2value = (ArrayWritable)mapItemValue2.get()[1];
            Assert.assertEquals("test value for field: tags", mapItemValue1value.get().length, 2);
            Assert.assertEquals("test value for field: tags", mapItemValue2value.get().length, 2);
            Assert.assertEquals("test value for field: tags[\"mapItem1\"].item1", mapItemValue1value.get()[0].toString(), "item" + currentRecordNo);
            Assert.assertEquals("test value for field: tags[\"mapItem2\"].item1", mapItemValue2value.get()[0].toString(), "item2" + currentRecordNo);
            Assert.assertEquals("test value for field: tags[\"mapItem1\"].item2", mapItemValue1value.get()[1].toString(), "item" + currentRecordNo + recordCommitTimeSuffix);
            Assert.assertEquals("test value for field: tags[\"mapItem2\"].item2", mapItemValue2value.get()[1].toString(), "item2" + currentRecordNo + recordCommitTimeSuffix);

            // Assert type RECORD
            ArrayWritable recordItem = (ArrayWritable)values[13];
            Writable[] nestedRecord = recordItem.get();
            Assert.assertEquals("test value for field: testNestedRecord.isAdmin", ((BooleanWritable)nestedRecord[0]).get(), false);
            Assert.assertEquals("test value for field: testNestedRecord.userId", nestedRecord[1].toString(), "UserId" + currentRecordNo + recordCommitTimeSuffix);

            // Assert type ARRAY
            ArrayWritable arrayValue = (ArrayWritable)values[14];
            Writable[] arrayValues = arrayValue.get();
            for (int i = 0; i < arrayValues.length; i++) {
                Assert.assertEquals("test value for field: stringArray", arrayValues[i].toString(), "stringArray" + i + recordCommitTimeSuffix);
            }
        }
    }
}
