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

package org.apache.hudi.hadoop;

import org.apache.hudi.FileSliceReader;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMergedLogRecordScanner;

public class HoodieHiveFileSliceReader implements FileSliceReader {

  private final FileSplit split;
  private final Configuration conf;
  private final Schema schema;
  private final JobConf jobConf;

  public HoodieHiveFileSliceReader(FileSplit split, Configuration conf, Schema schema, JobConf jobConf) {
    this.split = split;
    this.conf = conf;
    this.schema = schema;
    this.jobConf = jobConf;
  }

  @Override
  public FileSliceReader project(InternalSchema schema) {
    return null;
  }

  @Override
  public FileSliceReader project(Schema schema) {
    return null;
  }

  @Override
  public FileSliceReader pushDownFilters(Set<String> filters) {
    return null;
  }

  @Override
  public FileSliceReader readingMode(HoodieTableQueryType queryType) {
    return null;
  }

  @Override
  public Iterator<HoodieRecord> open(FileSlice fileSlice) {
    // get base file reader
    try (HoodieFileReader baseFileReader = HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.HIVE).getFileReader(conf, split.getPath())) {
      Iterator<HoodieRecord> baseRecordIterator = baseFileReader.getRecordIterator(schema);
      if (split instanceof RealtimeSplit) {
        HoodieMergedLogRecordScanner logRecordScanner = getMergedLogRecordScanner((RealtimeSplit) split, jobConf, schema);
        while (baseRecordIterator.hasNext()) {
          logRecordScanner.processNextRecord(baseRecordIterator.next());
        }
        return logRecordScanner.iterator();
      } else {
        return baseRecordIterator;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to open file slice: " + fileSlice);
    }
  }

  @Override
  public void close() throws Exception {

  }
}
