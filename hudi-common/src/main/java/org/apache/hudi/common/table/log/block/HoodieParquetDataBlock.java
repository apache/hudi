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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieHFileReader;
import org.apache.hudi.io.storage.HoodieParquetReader;
import org.apache.hudi.io.storage.HoodieParquetStreamReader;
import org.apache.hudi.io.storage.HoodieParquetStreamWriter;

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.TypePath;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 * It is used with the Parquet base file format.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  public HoodieParquetDataBlock(@Nonnull Map<HeaderMetadataType, String> logBlockHeader,
                                @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
                                @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation, @Nonnull Option<byte[]> content,
                                FSDataInputStream inputStream, boolean readBlockLazily) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
  }

  public HoodieParquetDataBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
                                boolean readBlockLazily, long position, long blockSize, long blockEndpos, Schema readerSchema,
                                Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)), readerSchema, header,
        footer);
  }

  public HoodieParquetDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header) {
    super(records, header, new HashMap<>());
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords() throws IOException {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        Integer.parseInt("60000"),//HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES.defaultValue()),
        Double.parseDouble("0.000000001"),//HoodieIndexConfig.BLOOM_FILTER_FPP.defaultValue()),
        Integer.parseInt("100000"),//HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue()),
        BloomFilterTypeCode.SIMPLE.name());//HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE.defaultValue());

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, filter);

    HoodieAvroParquetConfig avroParquetConfig = new HoodieAvroParquetConfig(writeSupport, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 1024 * 1024 * 1024,
        new Configuration(), Double.parseDouble(String.valueOf(0.1)));//HoodieStorageConfig.PARQUET_COMPRESSION_RATIO.defaultValue()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream outputStream = new FSDataOutputStream(baos, null);

    HoodieParquetStreamWriter<IndexedRecord> parquetWriter = new HoodieParquetStreamWriter<>(
        outputStream,
        avroParquetConfig);

    Iterator<IndexedRecord> itr = records.iterator();
    boolean useIntegerKey = false;
    int key = 0;
    int keySize = 0;
    Schema.Field keyField = records.get(0).getSchema().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    if (keyField == null) {
      // Missing key metadata field so we should use an integer sequence key
      useIntegerKey = true;
      keySize = (int) Math.ceil(Math.log(records.size())) + 1;
    }
    while (itr.hasNext()) {
      IndexedRecord record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keySize + "s", key++);
      } else {
        recordKey = record.get(keyField.pos()).toString();
      }
      parquetWriter.writeAvro(recordKey, record);
    }

    outputStream.flush();
    outputStream.close();
    parquetWriter.close();

    return baos.toByteArray();
  }

  public List<ArrayWritable> getArrayWritableRecords() {
    JobConf inlineConf = new JobConf();
    List<ArrayWritable> toReturn = new ArrayList<>();

    try {
      // Step 1: Make the config
      List<Schema.Field> fields = schema.getFields();

      String names = fields.stream()
          .map(f -> f.name()).collect(Collectors.joining(","));
      String positions = fields.stream()
          .map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));

      String hiveColumnTypes ="string,string,string,string,string," + "string,bigint,string,string,struct<id:int,login:string,gravatar_id:string,url:string,avatar_url:string,display_login:string>," +
          "struct<id:int,name:string,url:string>," +
          "string,struct<id:int,login:string,gravatar_id:string,url:string,avatar_url:string,display_login:string>," +
          "bigint,boolean";
      String hiveColumnNames = "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,date,timestamp,id,type,actor,repo,payload,org,created_at,public";

      inlineConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
      inlineConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
      inlineConf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
      inlineConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
      inlineConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);

      Configuration conf = new Configuration();
      conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
      conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes);
      conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
      conf.set(IOConstants.COLUMNS, hiveColumnNames);
      conf.get(IOConstants.COLUMNS_TYPES, hiveColumnTypes);

      inlineConf.addResource(conf);

      Path inlinePath = InLineFSUtils.getInlineFilePath(
          getBlockContentLocation().get().getLogFile().getPath(),
          getBlockContentLocation().get().getLogFile().getPath().getFileSystem(inlineConf).getScheme(),
          getBlockContentLocation().get().getContentPositionInLogFile(),
          getBlockContentLocation().get().getBlockSize());

      /*System.out.println("WNI HoodieParquetDataBlock getRecords " + getBlockContentLocation().get().getLogFile().getPath()
            + " " + getBlockContentLocation().get().getLogFile().getPath().getFileSystem(inlineConf).getScheme()
            + " " + getBlockContentLocation().get().getContentPositionInLogFile()
            + " " + getBlockContentLocation().get().getBlockSize()
            + " " + inlinePath.toString());*/

      inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
      inlineConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      RecordReader<NullWritable, ArrayWritable> reader =
          new MapredParquetInputFormat().getRecordReader(
          new FileSplit(inlinePath, 0,
              getBlockContentLocation().get().getBlockSize(), (String[]) null),
          inlineConf, null);

      // use reader to read base Parquet File and log file, merge in flight and return latest commit
      // here all 100 records should be updated, see above
      NullWritable key = reader.createKey();
      ArrayWritable value = reader.createValue();
      while (reader.next(key, value)) {
        toReturn.add(value);
      }
      reader.close();
    } catch (Exception exception) {
        System.out.println("WNI Fatal " + exception);
      }
    return toReturn;
  }


  @Override
  public List<IndexedRecord> getRecords() {
    Configuration inlineConf = new Configuration();
    List<IndexedRecord> toReturn = new ArrayList<>();

    try {
      Path inlinePath = InLineFSUtils.getInlineFilePath(
          getBlockContentLocation().get().getLogFile().getPath(),
          getBlockContentLocation().get().getLogFile().getPath().getFileSystem(inlineConf).getScheme(),
          getBlockContentLocation().get().getContentPositionInLogFile(),
          getBlockContentLocation().get().getBlockSize());

      /*System.out.println("WNI HoodieParquetDataBlock getRecords " + getBlockContentLocation().get().getLogFile().getPath()
            + " " + getBlockContentLocation().get().getLogFile().getPath().getFileSystem(inlineConf).getScheme()
            + " " + getBlockContentLocation().get().getContentPositionInLogFile()
            + " " + getBlockContentLocation().get().getBlockSize()
            + " " + inlinePath.toString());*/

      inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
      inlineConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      HoodieParquetReader<IndexedRecord> parquetReader = new HoodieParquetReader<>(inlineConf, inlinePath);
      Iterator<IndexedRecord> recordIterator = parquetReader.getRecordIterator(schema);

      while (recordIterator.hasNext()) {
        toReturn.add(recordIterator.next());
      }

    } catch (Exception exception) {
      System.out.println("WNI Fatal " + exception);
    }
    return toReturn;
  }

  // TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  // TODO (na) - Implement a recordItr instead of recordList
  @Override
  protected void deserializeRecords() throws IOException {
    throw new IOException("Not implemented");
  }
}
