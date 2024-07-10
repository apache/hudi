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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public class ExecutionStrategyUtil {

  /**
   * Transform IndexedRecord into HoodieRecord.
   *
   * @param indexedRecord indexedRecord.
   * @param writeConfig writeConfig.
   * @return hoodieRecord.
   * @param <T>
   */
  public static <T> HoodieRecord<T> transform(IndexedRecord indexedRecord,
      HoodieWriteConfig writeConfig) {

    GenericRecord record = (GenericRecord) indexedRecord;
    Option<BaseKeyGenerator> keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig);

    String key = KeyGenUtils.getRecordKeyFromGenericRecord(record, keyGeneratorOpt);
    String partition = KeyGenUtils.getPartitionPathFromGenericRecord(record, keyGeneratorOpt);
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }
}
