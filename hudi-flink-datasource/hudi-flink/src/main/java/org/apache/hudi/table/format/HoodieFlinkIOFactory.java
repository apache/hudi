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

package org.apache.hudi.table.format;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.storage.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;

/**
 * Creates readers and writers for Flink record payloads
 */
public class HoodieFlinkIOFactory extends HoodieHadoopIOFactory {
  public HoodieFlinkIOFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.FLINK) {
      return new HoodieRowDataFileWriterFactory(storage);
    }
    return super.getWriterFactory(recordType);
  }

  @Override
  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.FLINK) {
      return new HoodieRowDataFileReaderFactory(storage);
    }
    return super.getReaderFactory(recordType);
  }
}
