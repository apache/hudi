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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Iterator;

public class HoodieLogFileIterator implements Iterator<HoodieRecord> {

  private final FileSplit split;
  private final JobConf jobConf;
  private final HoodieFileReader baseFileReader;
  private final Iterator<HoodieRecord> baseFileIterator;
  private final Schema dataSchema;
  private final Schema readerSchema;

  public HoodieLogFileIterator(FileSplit split, JobConf jobConf, HoodieFileReader baseFileReader, Schema dataSchema, Schema readerSchema) {
    this.split = split;
    this.jobConf = jobConf;
    this.baseFileReader = baseFileReader;
    this.dataSchema = dataSchema;
    this.readerSchema = readerSchema;
    try {
      this.baseFileIterator = baseFileReader.getRecordIterator(readerSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get base records iterator.", e);
    }
  }

  @Override
  public boolean hasNext() {
    if (baseFileIterator.hasNext()) {
      HoodieRecord baseRecord = baseFileIterator.next();
      // TODO: meging iterator
    }
    return false;
  }

  @Override
  public HoodieRecord next() {
    return null;
  }
}
