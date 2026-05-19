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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.storage.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.storage.HoodieStorage;

/**
 * Creates readers and writers for SPARK and AVRO record payloads
 */
public class HoodieSparkIOFactory extends HoodieHadoopIOFactory {

  public HoodieSparkIOFactory(HoodieStorage storage) {
    super(storage);
  }

  public static HoodieSparkIOFactory getHoodieSparkIOFactory(HoodieStorage storage) {
    return new HoodieSparkIOFactory(storage);
  }

  @Override
  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      return new HoodieSparkFileReaderFactory(storage);
    }
    return super.getReaderFactory(recordType);
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      return new HoodieSparkFileWriterFactory(storage);
    }
    return super.getWriterFactory(recordType);
  }
}
