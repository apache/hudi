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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

public class HoodieHFileRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private long count = 0;
  private final ArrayWritable valueObj;
  private HoodieFileReader reader;
  private ClosableIterator<HoodieRecord<IndexedRecord>> recordIterator;
  private final HoodieSchema schema;

  public HoodieHFileRecordReader(Configuration conf, InputSplit split, JobConf job) throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    StoragePath path = convertToStoragePath(fileSplit.getPath());
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(conf);
    HoodieConfig hoodieConfig = getReaderConfigs(storageConf);
    reader = HoodieIOFactory.getIOFactory(HoodieStorageUtils.getStorage(path, storageConf)).getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(hoodieConfig, path, HoodieFileFormat.HFILE, Option.empty());

    schema = reader.getSchema();
    valueObj = new ArrayWritable(Writable.class, new Writable[schema.getFields().size()]);
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    if (recordIterator == null) {
      recordIterator = reader.getRecordIterator(schema);
    }

    if (!recordIterator.hasNext()) {
      return false;
    }

    IndexedRecord record = recordIterator.next().getData();
    ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, schema.toAvroSchema());
    value.set(aWritable.get());
    count++;
    return true;
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (recordIterator != null) {
      recordIterator.close();
      recordIterator = null;
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 1.0f * count / reader.getTotalRecords();
  }
}
