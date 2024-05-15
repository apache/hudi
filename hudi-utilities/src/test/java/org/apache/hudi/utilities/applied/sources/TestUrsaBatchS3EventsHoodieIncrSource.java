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

package org.apache.hudi.utilities.applied.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.applied.common.TestCommon;
import org.apache.hudi.utilities.applied.common.UrsaBatchInfo;
import org.apache.hudi.utilities.applied.schema.UrsaBatchProtoSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.hudi.utilities.applied.schema.UrsaWrapperSchema.READER_PARQUET_SCHEMA;
import static org.apache.hudi.utilities.applied.sources.UrsaBatchS3EventsHoodieIncrSource.BUCKET_JOIN_COLUMN;
import static org.apache.hudi.utilities.applied.sources.UrsaBatchS3EventsHoodieIncrSource.FILE_KEY_JOIN_COLUMN;

public class TestUrsaBatchS3EventsHoodieIncrSource extends UtilitiesTestBase {

  private static class TestUrsaBatchProtoSchemaProvider extends UrsaBatchProtoSchemaProvider {

    public TestUrsaBatchProtoSchemaProvider(TypedProperties props) {
      super(props);
    }

    @Override
    public void refresh() {
    }

    public File getProtoLatestLocalSchemaFile() {
      return new File(TestCommon.getPersonFileDescriptorPath());
    }

    public String getSerializedSourceColumn() {
      return "serialized_customer_frame";
    }

    public String getProtoMessageTypeName() {
      return "Person";
    }
  }

  @BeforeAll
  public static void initialize() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
    String schemaStr = READER_PARQUET_SCHEMA;
    Schema schema = new Schema.Parser().parse(schemaStr);
    UrsaBatchInfo batchInfo = TestCommon.getTestBatchInfo();
    for (int i = 0; i < 5; i++) {
      String[] parts = batchInfo.getFiles().get(i).split("/");
      String filename = TestCommon.getPersonParquetDirPath() + parts[parts.length - 1];
      // Define the path to the output Parquet file
      Path path = new Path(filename);

      // Create the ParquetWriter
      ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
          .withSchema(schema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
          .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .build();

      // Create a Person proto
      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        TestCommon.PERSONS[i].writeTo(outputStream);
        byte[] outputBytes = outputStream.toByteArray();
        // Create a record to write
        GenericRecord record = new GenericData.Record(schema);
        record.put("run_uuid", TestCommon.TEST_UUID);
        record.put("timestamp_ns", TestCommon.TEST_TIMESTAMPS_N[i]);
        record.put("serialized_customer_frame", ByteBuffer.wrap(outputBytes));
        // Write the record to the Parquet file
        writer.write(record);
        // Close the writer
        writer.close();
      }
    }
    System.out.println("Parquet files written successfully.");
  }

  private UrsaBatchS3EventsHoodieIncrSource setupSource() {
    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.streamer.source.hoodieincr.path", "dummy_path");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.deserialized.col.parent", "CustomFrame");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.proto.serialized.col", "serialized_customer_frame");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.file.descriptor.set.message.type.name", "Person");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.file.descriptor.set.s3.prefix", "dummy/prefix");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.file.descriptor.set.s3.bucket.region", "us-west-1");
    properties.put("hoodie.streamer.schemaprovider.ursa.proto.file.descriptor.set.s3.bucket.name", "dummy_bucket");
    return new UrsaBatchS3EventsHoodieIncrSource(properties, jsc, sparkSession, new TestUrsaBatchProtoSchemaProvider(properties));
  }

  @Test
  public void testSource() {
    String parquetBasePath = TestCommon.getPersonParquetDirPath();
    UrsaBatchS3EventsHoodieIncrSource source = setupSource();
    Pair<Pair<TypedProperties, UrsaBatchInfo>, Dataset<Row>> input = TestCommon.generateUrsaBatchS3Events(sparkSession);
    UrsaBatchS3EventsSource s3EventsSource = new UrsaBatchS3EventsSource(input.getLeft().getLeft(), jsc, sparkSession, null);
    Dataset<Row> filesDf = s3EventsSource.fetchParquetFilesFromBatches(input.getRight(),
            new TestUrsaBatchS3EventsSource.TestBatchRecordGenFunction(input.getLeft().getRight()),
            new TestUrsaBatchS3EventsSource.TestFileValidatorFunction())
            .withColumn(BUCKET_JOIN_COLUMN, functions.lit("dummy"))
            .withColumn(FILE_KEY_JOIN_COLUMN, functions.substring_index(functions.col("s3.object.key"), "/", -1));
    Dataset<Row> sourceData = sparkSession.read().parquet(parquetBasePath)
            .withColumn(BUCKET_JOIN_COLUMN, functions.lit("dummy"))
            .withColumn(FILE_KEY_JOIN_COLUMN, functions.substring_index(functions.input_file_name(), "/", -1));
    Dataset<Row> res = source.joinAndDeserialize(sourceData, filesDf).orderBy(functions.col("CustomFrame.id"));
    res.printSchema();
    res.show(10, false);
    List<Row> rowList = res.collectAsList();
    Assertions.assertEquals(TestCommon.PERSONS.length, rowList.size());
    String[] expFileUrls = TestCommon.getAbsolutePaths("s3://" + TestCommon.TEST_BUCKET + "/");
    for (int i= 0; i < TestCommon.PERSONS.length; i++) {
      Row person = rowList.get(i).getStruct(rowList.get(i).fieldIndex("CustomFrame"));
      if (i  == 0) {
        Assertions.assertNull(person.getAs("id"));
      } else {
        Assertions.assertEquals(new Integer(TestCommon.PERSONS[i].getId()), person.getAs("id"));
      }
      Assertions.assertEquals(TestCommon.PERSONS[i].getName(), person.getAs("name"));
      Assertions.assertEquals(TestCommon.PERSONS[i].getEmail(), person.getAs("email"));
      Assertions.assertEquals(TestCommon.TEST_UUID, rowList.get(i).getAs("run_uuid"));
      Assertions.assertEquals(new Long(TestCommon.TEST_TIMESTAMPS_N[i]), rowList.get(i).getAs("timestamp_ns"));
      Assertions.assertEquals(TestCommon.TEST_UUID, rowList.get(i).getAs("_lake_batch_uuid"));
      Assertions.assertEquals(TestCommon.TEST_BUCKET_REGION, rowList.get(i).getAs("_lake_region"));
      Assertions.assertEquals(TestCommon.TEST_BATCH_BUCKET_NAME, rowList.get(i).getAs("_lake_bucket"));
      Assertions.assertEquals(TestCommon.TEST_SOURCE_TYPE, rowList.get(i).getAs("_lake_source_type"));
      Assertions.assertEquals(TestCommon.TEST_BATCH_BUCKET_KEY, rowList.get(i).getAs("_lake_batch_key"));
      Assertions.assertEquals(TestCommon.EXPECTED_BATCH_CREATED_TIMESTAMP_SEC, rowList.get(i).getAs("_lake_batch_created_timestamp_sec"));
      Assertions.assertEquals(TestCommon.EXPECTED_ORIGIN_CREATED_TIMESTAMP_SEC, rowList.get(i).getAs("_lake_origin_created_timestamp_sec"));
      Assertions.assertEquals(TestCommon.EXPECTED_ORIGIN_UPDATED_TIMESTAMP_SEC, rowList.get(i).getAs("_lake_origin_updated_timestamp_sec"));
      Assertions.assertEquals(expFileUrls[i], rowList.get(i).getAs("_lake_file_url"));
    }
  }
}
