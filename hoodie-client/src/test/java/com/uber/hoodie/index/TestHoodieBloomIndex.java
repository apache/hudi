/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;

import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class TestHoodieBloomIndex {
    private JavaSparkContext jsc = null;
    private String basePath = null;
    private transient final FileSystem fs;

    public TestHoodieBloomIndex() throws Exception {
        fs = FSUtils.getFs();
    }

    @Before
    public void init() throws IOException {
        // Initialize a local spark env
        SparkConf sparkConf = new SparkConf().setAppName("TestHoodieBloomIndex").setMaster("local[4]");
        jsc = new JavaSparkContext(sparkConf);
        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.init(basePath);
    }

    @Test
    public void testLoadUUIDsInMemory() throws IOException {
        // Create one RDD of hoodie record
        String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
        String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
        String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";

        String schemaStr =
            IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
        TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
        HoodieRecord record3 = new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
        TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
        HoodieRecord record4 = new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

        JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

        // Load to memory
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieBloomIndex index = new HoodieBloomIndex(config, jsc);
        Map<String, Iterable<String>> map = index.getPartitionToRowKeys(recordRDD);
        assertEquals(map.size(), 2);
        List<String> list1 = Lists.newArrayList(map.get("2016/01/31"));
        List<String> list2 = Lists.newArrayList(map.get("2015/01/31"));
        assertEquals(list1.size(), 3);
        assertEquals(list2.size(), 1);
    }

    @Test
    public void testLoadInvolvedFiles() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieBloomIndex index = new HoodieBloomIndex(config, jsc);

        // Create some partitions, and put some files
        // "2016/01/21": 0 file
        // "2016/04/01": 1 file (2_0_20160401010101.parquet)
        // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
        new File(basePath + "/2016/01/21").mkdirs();
        new File(basePath + "/2016/04/01").mkdirs();
        new File(basePath + "/2015/03/12").mkdirs();
        new File(basePath + "/2016/04/01/2_0_20160401010101.parquet").createNewFile();
        new File(basePath + "/2015/03/12/1_0_20150312101010.parquet").createNewFile();
        new File(basePath + "/2015/03/12/3_0_20150312101010.parquet").createNewFile();
        new File(basePath + "/2015/03/12/4_0_20150312101010.parquet").createNewFile();
        List<String> partitions = Arrays.asList("2016/01/21", "2016/04/01", "2015/03/12");
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);
        JavaPairRDD<String, String> rdd = index.loadInvolvedFiles(partitions, table);
        // Still 0, as no valid commit
        assertEquals(rdd.count(), 0);

        // Add some commits
        new File(basePath + "/.hoodie").mkdirs();
        new File(basePath + "/.hoodie/20160401010101.commit").createNewFile();
        new File(basePath + "/.hoodie/20150312101010.commit").createNewFile();
        metadata = new HoodieTableMetaClient(fs, basePath);
        rdd = index.loadInvolvedFiles(partitions, table);
        final List<Tuple2<String, String>> filesList = rdd.collect();
        assertEquals(filesList.size(), 4);

        // no longer sorted, but should have same files.
        Set<String> actualFiles = new HashSet<String>(){{
            add(filesList.get(0)._1 + "/" + filesList.get(0)._2);
            add(filesList.get(1)._1 + "/" + filesList.get(1)._2);
            add(filesList.get(2)._1 + "/" + filesList.get(2)._2);
            add(filesList.get(3)._1 + "/" + filesList.get(3)._2);
        }};

        Set<String> expected = new HashSet<String>() {{
            add("2016/04/01/2_0_20160401010101.parquet");
            add("2015/03/12/1_0_20150312101010.parquet");
            add("2015/03/12/3_0_20150312101010.parquet");
            add("2015/03/12/4_0_20150312101010.parquet");
        }};
        assertEquals(expected, actualFiles);
    }

    @Test
    public void testCheckUUIDsAgainstOneFile() throws IOException, InterruptedException, ClassNotFoundException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieBloomIndex index = new HoodieBloomIndex(config, jsc);

        String schemaStr =
            IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
        Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
        // Create some records to use
        String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
        String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
        String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
        TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
        HoodieRecord record3 = new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
        TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
        HoodieRecord record4 = new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);


        // We write record1, record2 to a parquet file, but the bloom filter contains (record1, record2, record3).
        BloomFilter filter = new BloomFilter(10000, 0.0000001);
        filter.add(record3.getRecordKey());
        String filename = writeParquetFile("2016/01/31", Arrays.asList(record1, record2), schema, filter, true);

        // The bloom filter contains 3 records
        assertTrue(filter.mightContain(record1.getRecordKey()));
        assertTrue(filter.mightContain(record2.getRecordKey()));
        assertTrue(filter.mightContain(record3.getRecordKey()));
        assertFalse(filter.mightContain(record4.getRecordKey()));

        // Compare with file
        List<String> uuids = Arrays.asList(record1.getRecordKey(), record2.getRecordKey(),
                record3.getRecordKey(), record4.getRecordKey());

        List<String> results = HoodieBloomIndexCheckFunction.checkCandidatesAgainstFile(uuids,
                new Path(basePath + "/2016/01/31/" + filename));
        assertEquals(results.size(), 2);
        assertTrue(results.get(0).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")
                || results.get(1).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
        assertTrue(results.get(0).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")
                || results.get(1).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
        // TODO(vc): Need more coverage on actual filenames
        //assertTrue(results.get(0)._2().equals(filename));
        //assertTrue(results.get(1)._2().equals(filename));
    }

    @Test
    public void testTagLocationWithEmptyRDD() throws Exception {
        // We have some records to be tagged (two different partitions)
        JavaRDD<HoodieRecord> recordRDD = jsc.emptyRDD();
        // Also create the metadata and config
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

        // Let's tag
        HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, jsc);

        try {
            bloomIndex.tagLocation(recordRDD, table);
        } catch (IllegalArgumentException e) {
            fail("EmptyRDD should not result in IllegalArgumentException: Positive number of slices required");
        }
    }


    @Test
    public void testTagLocation() throws Exception {
        // We have some records to be tagged (two different partitions)
        String schemaStr =
            IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
        Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));

        String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
        String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
        String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
        TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
        HoodieRecord record3 = new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
        TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
        HoodieRecord record4 = new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);
        JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

        // Also create the metadata and config
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

        // Let's tag
        HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, jsc);
        JavaRDD<HoodieRecord> taggedRecordRDD = bloomIndex.tagLocation(recordRDD, table);

        // Should not find any files
        for (HoodieRecord record : taggedRecordRDD.collect()) {
            assertTrue(!record.isCurrentLocationKnown());
        }

        // We create three parquet file, each having one record. (two different partitions)
        String filename1 = writeParquetFile("2016/01/31", Arrays.asList(record1), schema, null, true);
        String filename2 = writeParquetFile("2016/01/31", Arrays.asList(record2), schema, null, true);
        String filename3 = writeParquetFile("2015/01/31", Arrays.asList(record4), schema, null, true);

        // We do the tag again
        metadata = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metadata, config);

        taggedRecordRDD = bloomIndex.tagLocation(recordRDD, table);

        // Check results
        for (HoodieRecord record : taggedRecordRDD.collect()) {
            if (record.getRecordKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename1)));
            } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename2)));
            } else if (record.getRecordKey().equals("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
                assertTrue(!record.isCurrentLocationKnown());
            } else if (record.getRecordKey().equals("4eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename3)));
            }
        }
    }

    @Test
    public void testCheckExists() throws Exception {
        // We have some records to be tagged (two different partitions)
        String schemaStr =
            IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
        Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));

        String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
        String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
        String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieKey key1 = new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath());
        HoodieRecord record1 = new HoodieRecord(key1, rowChange1);
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        HoodieKey key2 = new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath());
        HoodieRecord record2 = new HoodieRecord(key2, rowChange2);
        TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
        HoodieKey key3 = new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath());
        HoodieRecord record3 = new HoodieRecord(key3, rowChange3);
        TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
        HoodieKey key4 = new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath());
        HoodieRecord record4 = new HoodieRecord(key4, rowChange4);
        JavaRDD<HoodieKey> keysRDD = jsc.parallelize(Arrays.asList(key1, key2, key3, key4));

        // Also create the metadata and config
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

        // Let's tag
        HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, jsc);
        JavaPairRDD<HoodieKey, Optional<String>> taggedRecordRDD = bloomIndex.fetchRecordLocation(keysRDD, table);

        // Should not find any files
        for (Tuple2<HoodieKey, Optional<String>> record : taggedRecordRDD.collect()) {
            assertTrue(!record._2.isPresent());
        }

        // We create three parquet file, each having one record. (two different partitions)
        String filename1 = writeParquetFile("2016/01/31", Arrays.asList(record1), schema, null, true);
        String filename2 = writeParquetFile("2016/01/31", Arrays.asList(record2), schema, null, true);
        String filename3 = writeParquetFile("2015/01/31", Arrays.asList(record4), schema, null, true);

        // We do the tag again
        metadata = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metadata, config);
        taggedRecordRDD = bloomIndex.fetchRecordLocation(keysRDD, table);

        // Check results
        for (Tuple2<HoodieKey, Optional<String>> record : taggedRecordRDD.collect()) {
            if (record._1.getRecordKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record._2.isPresent());
                Path path1 = new Path(record._2.get());
                assertEquals(FSUtils.getFileId(filename1), FSUtils.getFileId(path1.getName()));
            } else if (record._1.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record._2.isPresent());
                Path path2 = new Path(record._2.get());
                assertEquals(FSUtils.getFileId(filename2), FSUtils.getFileId(path2.getName()));
            } else if (record._1.getRecordKey().equals("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
                assertTrue(!record._2.isPresent());
            } else if (record._1.getRecordKey().equals("4eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record._2.isPresent());
                Path path3 = new Path(record._2.get());
                assertEquals(FSUtils.getFileId(filename3), FSUtils.getFileId(path3.getName()));
            }
        }
    }


    @Test
    public void testBloomFilterFalseError() throws IOException, InterruptedException {
        // We have two hoodie records
        String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";

        // We write record1 to a parquet file, using a bloom filter having both records
        String schemaStr =
            IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
        Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));

        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);

        BloomFilter filter = new BloomFilter(10000, 0.0000001);
        filter.add(record2.getRecordKey());
        String filename = writeParquetFile("2016/01/31", Arrays.asList(record1), schema, filter, true);
        assertTrue(filter.mightContain(record1.getRecordKey()));
        assertTrue(filter.mightContain(record2.getRecordKey()));

        // We do the tag
        JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record1, record2));
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

        HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, jsc);
        JavaRDD<HoodieRecord> taggedRecordRDD = bloomIndex.tagLocation(recordRDD, table);

        // Check results
        for (HoodieRecord record : taggedRecordRDD.collect()) {
            if (record.getKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
                assertTrue(record.getCurrentLocation().getFileId().equals(FSUtils.getFileId(filename)));
            } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
                assertFalse(record.isCurrentLocationKnown());
            }
        }
    }

    private String writeParquetFile(String partitionPath, List<HoodieRecord> records, Schema schema,
                                    BloomFilter filter, boolean createCommitTime) throws IOException, InterruptedException {
        Thread.sleep(1000);
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String fileId = UUID.randomUUID().toString();
        String filename = FSUtils.makeDataFileName(commitTime, 1, fileId);

        return writeParquetFile(partitionPath, filename, records, schema, filter, createCommitTime);
    }

    private String writeParquetFile(String partitionPath, String filename, List<HoodieRecord> records, Schema schema,
                                    BloomFilter filter, boolean createCommitTime) throws IOException {
        if (filter == null) {
            filter = new BloomFilter(10000, 0.0000001);
        }
        HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, filter);
        ParquetWriter writer = new ParquetWriter(new Path(basePath + "/" + partitionPath + "/" + filename),
                writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE);
        int seqId = 1;
        String commitTime = FSUtils.getCommitTime(filename);
        for (HoodieRecord record : records) {
            GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema);
            HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, commitTime, "" + seqId++);
            HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), filename);
            writer.write(avroRecord);
            filter.add(record.getRecordKey());
        }
        writer.close();

        if (createCommitTime) {
            // Also make sure the commit is valid
            new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME).mkdirs();
            new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + ".commit").createNewFile();
        }
        return filename;
    }

    @After
    public void clean() {
        if (jsc != null) {
            jsc.stop();
        }
        if (basePath != null) {
            new File(basePath).delete();
        }
    }
}
