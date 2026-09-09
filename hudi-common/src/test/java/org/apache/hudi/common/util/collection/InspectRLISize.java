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

package org.apache.hudi.common.util.collection;

/*
 * Quick utility: print the kryo-serialized size of an RLI record and its deflated size.
 * Run: mvn -pl hudi-common exec:java -Dexec.classpathScope=test \
 *      -Dexec.mainClass=org.apache.hudi.common.util.collection.InspectRLISize
 */

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.io.ByteArrayOutputStream;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class InspectRLISize {
  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 5; i++) {
      String recordKey = "user_record_" + i;
      String partition = "date=2026-05-22";
      String fileId = UUID.randomUUID().toString() + "-0";
      String instantTime = "20260522120000";
      HoodieRecord<HoodieMetadataPayload> rec =
          HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId, instantTime, 0);
      byte[] serialized = SerializationUtils.serialize(rec);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Deflater d = new Deflater(Deflater.BEST_COMPRESSION);
      try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, d)) {
        dos.write(serialized);
      }
      d.end();
      System.out.printf("record %d: kryoBytes=%d  deflatedBytes=%d  ratio=%.2fx%n",
          i, serialized.length, baos.size(),
          (double) serialized.length / baos.size());
    }
  }
}
